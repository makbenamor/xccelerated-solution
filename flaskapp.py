import psycopg2

# Define connection parameters
DATABASE_SETTINGS = {
    "database": "xcceleratedpostgresdb",
    "user": "postgres",
    "password": "AzerKimo2022@@@",
    "host": "172.17.0.2",
    "port": "5432"
}

# Create a connection and cursor object
connection = psycopg2.connect(**DATABASE_SETTINGS)
cursor = connection.cursor()

# SQL to get median visits before order
median_visits_sql = """
WITH OrdersSessions AS (
    SELECT customer_id, MIN(session_timestamp) as first_order_time
    FROM sessions
    WHERE event_type = 'ADD_PRODUCT_TO_CART'  -- Assuming this indicates a purchase
    GROUP BY customer_id
)
SELECT 
    customer_id, 
    COUNT(session_id) as total_sessions_before_order
FROM sessions
JOIN OrdersSessions ON sessions.customer_id = OrdersSessions.customer_id
WHERE session_timestamp < first_order_time
GROUP BY sessions.customer_id
"""

# SQL to get median session duration before order
median_duration_sql = """
WITH OrdersSessions AS (
    SELECT customer_id, MIN(session_timestamp) as first_order_time
    FROM sessions
    WHERE event_type = 'ADD_PRODUCT_TO_CART'  -- Assuming this indicates a purchase
    GROUP BY customer_id
)
SELECT 
    customer_id, 
    AVG(session_end_time - session_start_time) as avg_session_duration
FROM sessions
JOIN OrdersSessions ON sessions.customer_id = OrdersSessions.customer_id
WHERE session_timestamp < first_order_time
GROUP BY sessions.customer_id
"""

from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/metrics/orders", methods=["GET"])
def metrics_orders():
    # Fetch median visits before order
    cursor.execute(median_visits_sql)
    visits_data = cursor.fetchall()
    visits_data = [item[1] for item in visits_data]
    median_visits = sorted(visits_data)[len(visits_data) // 2] if visits_data else None

    # Fetch median session duration before order
    cursor.execute(median_duration_sql)
    duration_data = cursor.fetchall()
    duration_data = [item[1].total_seconds() / 60 for item in duration_data]  # Convert to minutes
    median_duration = sorted(duration_data)[len(duration_data) // 2] if duration_data else None
    
    return jsonify({
        "median_visits_before_order": median_visits,
        "median_session_duration_minutes_before_order": median_duration
    })

# Run the server on port 5000
if __name__ == "__main__":
    app.run(port=5000)
