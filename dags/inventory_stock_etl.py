from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="inventory_stock_etl",
    start_date=datetime(2026, 8, 26),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:


    check_daily_file = BashOperator(
        task_id="check_daily_file",
        bash_command="""
        hdfs dfs -test -e \
        /data/inventory/raw/event_date={{ ds }}/stock_movements_{{ ds }}.csv
        """
    )


    register_partition = BashOperator(
        task_id="register_partition",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        ALTER TABLE inventory.raw_stock_movements
        ADD IF NOT EXISTS
        PARTITION (event_date='{{ ds }}')
        LOCATION '/data/inventory/raw/event_date={{ ds }}';
        "
        """
    )


    load_clean_movements = BashOperator(
        task_id="load_clean_movements",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        INSERT OVERWRITE TABLE inventory.clean_stock_movements
        PARTITION (event_date='{{ ds }}')
        SELECT
            movement_id,
            product_id,
            warehouse_id,
            movement_type,
            quantity,
            unit_cost,
            movement_time
        FROM inventory.raw_stock_movements
        WHERE event_date='{{ ds }}'
          AND source_event_date='{{ ds }}'
          AND quantity > 0
          AND unit_cost >= 0
          AND movement_type IN ('IN','OUT');
        "
        """
    )


    calculate_stock_balance = BashOperator(
        task_id="calculate_stock_balance",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        INSERT OVERWRITE TABLE inventory.stock_balance
        PARTITION (event_date='{{ ds }}')
        SELECT
            m.product_id,
            m.warehouse_id,
            m.product_name,
            m.opening_stock,

            SUM(
                CASE
                    WHEN c.movement_type='IN'
                    THEN c.quantity
                    ELSE 0
                END
            ),

            SUM(
                CASE
                    WHEN c.movement_type='OUT'
                    THEN c.quantity
                    ELSE 0
                END
            ),

            m.opening_stock +
            SUM(
                CASE
                    WHEN c.movement_type='IN' THEN c.quantity
                    WHEN c.movement_type='OUT' THEN -c.quantity
                    ELSE 0
                END
            ),

            m.reorder_level

        FROM inventory.inventory_master m

        LEFT JOIN inventory.clean_stock_movements c
            ON m.product_id = c.product_id
           AND m.warehouse_id = c.warehouse_id
           AND c.event_date <= '{{ ds }}'

        GROUP BY
            m.product_id,
            m.warehouse_id,
            m.product_name,
            m.opening_stock,
            m.reorder_level;
        "
        """
    )


    generate_low_stock_alerts = BashOperator(
        task_id="generate_low_stock_alerts",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        INSERT OVERWRITE TABLE inventory.low_stock_alerts
        PARTITION (event_date='{{ ds }}')
        SELECT
            product_id,
            warehouse_id,
            product_name,
            current_stock,
            reorder_level,
            CASE
                WHEN current_stock <= 0 THEN 'OUT_OF_STOCK'
                WHEN current_stock <= reorder_level THEN 'LOW_STOCK'
            END
        FROM inventory.stock_balance
        WHERE event_date='{{ ds }}'
          AND current_stock <= reorder_level;
        "
        """
    )


    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command="""
        COUNT_RESULT=$(beeline \
        -u jdbc:hive2://hive-server:10000 \
        --silent=true \
        --showHeader=false \
        --outputformat=tsv2 \
        -e "
        SELECT COUNT(*)
        FROM inventory.clean_stock_movements
        WHERE event_date='{{ ds }}';
        " 2>/dev/null | grep -E '^[0-9]+$' | tail -1)

        echo "Clean movement count: $COUNT_RESULT"

        test -n "$COUNT_RESULT"
        test "$COUNT_RESULT" -gt 0
        """
    )


    check_daily_file \
        >> register_partition \
        >> load_clean_movements \
        >> calculate_stock_balance \
        >> generate_low_stock_alerts \
        >> data_quality_check
