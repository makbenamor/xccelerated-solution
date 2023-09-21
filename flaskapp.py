import datetime
import psycopg2
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

# Database configuration
DB_NAME = "xcceleratedpostgresdb"
DB_USER = "postgres"
DB_PASSWORD = "AzerKimo2022@@@"  # Consider using environment variables for sensitive info
DB_HOST = "localhost"
DB_PORT = "5432"

# Create Flask app
app = Flask(__name__)

# SQLAlchemy configuration for PostgreSQL
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize SQLAlchemy for the app
db = SQLAlchemy(app)


# Model for the 'sessions' table
class Session(db.Model):
    __tablename__ = 'sessions'
    __table_args__ = {'schema': 'public'}
    session_id = db.Column(db.Integer, primary_key=True)
    customer_id = db.Column(db.Integer)
    start_time = db.Column(db.DateTime)
    end_time = db.Column(db.DateTime)
    duration = db.Column(db.Integer)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


def get_db_connection():
    """
    Establish and return a connection to the PostgreSQL database.
    """
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


@app.route('/metrics/orders', methods=['GET'])
def get_order_metrics():
    """
    Return metrics on median visits and session duration before an order.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # SQL queries to calculate metrics
        median_visits_query = """
        SELECT
            percentile_cont(0.5) WITHIN GROUP (ORDER BY visits) AS median_visits
        FROM (
            SELECT
                customer_id,
                COUNT(DISTINCT session_id) AS visits
            FROM
                sessions
            WHERE
                customer_id IS NOT NULL
            GROUP BY
                customer_id
        ) subquery
        """
        
        median_duration_query = """
        SELECT
            percentile_cont(0.5) WITHIN GROUP (ORDER BY duration) AS median_duration_minutes
        FROM (
            SELECT
                customer_id,
                MIN(duration) AS duration
            FROM
                sessions
            WHERE
                customer_id IS NOT NULL
            GROUP BY
                customer_id
        ) subquery
        """

        cursor.execute(median_visits_query)
        median_visits = cursor.fetchone()[0]

        cursor.execute(median_duration_query)
        median_duration_minutes = cursor.fetchone()[0]

        metrics = {
            "median_visits_before_order": median_visits,
            "median_session_duration_minutes_before_order": median_duration_minutes
        }

    except psycopg2.Error as e:
        metrics = {"error": str(e)}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return jsonify(metrics)


if __name__ == "__main__":
    app.run(debug=True)
