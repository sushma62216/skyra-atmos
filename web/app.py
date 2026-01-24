"""
Skyra Atmos - Weather Analytics Web Application
"""

import os
from flask import Flask, render_template, jsonify
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Snowflake connection config
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT', 'yafshsp-yob78287'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': 'SKYRA_ATMOS',
    'database': 'WEATHER_ANALYTICS',
    'schema': 'RAW'
}


def get_snowflake_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


@app.route('/')
def home():
    """Home page"""
    return render_template('index.html')


@app.route('/api/stats')
def get_stats():
    """Get overall statistics"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Get counts
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM RAW_STATIONS) as station_count,
                (SELECT COUNT(*) FROM RAW_COUNTRIES) as country_count,
                (SELECT COUNT(*) FROM RAW_DAILY_OBSERVATIONS) as observation_count,
                (SELECT MAX(DATE) FROM RAW_DAILY_OBSERVATIONS) as latest_date
        """)
        row = cur.fetchone()

        stats = {
            'stations': row[0],
            'countries': row[1],
            'observations': row[2],
            'latest_date': row[3]
        }

        cur.close()
        conn.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/top-countries')
def get_top_countries():
    """Get top countries by station count"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                c.COUNTRY_NAME,
                COUNT(s.STATION_ID) as station_count
            FROM RAW_STATIONS s
            JOIN RAW_COUNTRIES c ON LEFT(s.STATION_ID, 2) = c.COUNTRY_CODE
            GROUP BY c.COUNTRY_NAME
            ORDER BY station_count DESC
            LIMIT 10
        """)

        countries = [{'name': row[0], 'stations': row[1]} for row in cur.fetchall()]

        cur.close()
        conn.close()
        return jsonify(countries)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/recent-observations')
def get_recent_observations():
    """Get recent observation summary"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                ELEMENT,
                COUNT(*) as count,
                AVG(DATA_VALUE) as avg_value
            FROM RAW_DAILY_OBSERVATIONS
            WHERE ELEMENT IN ('TMAX', 'TMIN', 'PRCP', 'SNOW')
            GROUP BY ELEMENT
            ORDER BY count DESC
        """)

        observations = [
            {'element': row[0], 'count': row[1], 'avg_value': round(row[2], 2) if row[2] else 0}
            for row in cur.fetchall()
        ]

        cur.close()
        conn.close()
        return jsonify(observations)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
