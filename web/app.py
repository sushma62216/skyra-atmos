"""
Skyra Atmos - Weather Analytics Web Application
"""

import os
from flask import Flask, render_template, jsonify
import snowflake.connector

app = Flask(__name__)

# Snowflake connection config (strip whitespace from env vars)
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT', 'mrbhvfe-ozb46267').strip(),
    'user': os.getenv('SNOWFLAKE_USER', '').strip(),
    'password': os.getenv('SNOWFLAKE_PASSWORD', '').strip(),
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


# ============================================================
# CLIMATE TIME MACHINE
# ============================================================

@app.route('/time-machine')
def time_machine():
    """Climate Time Machine page"""
    return render_template('time_machine.html')


@app.route('/api/time-machine/<date>')
def get_weather_on_date(date):
    """Get weather data for a specific date (format: YYYYMMDD)"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Get weather summary for the date
        cur.execute("""
            SELECT
                COUNT(DISTINCT o.ID) as station_count,
                COUNT(*) as observation_count,
                AVG(CASE WHEN o.ELEMENT = 'TMAX' THEN o.DATA_VALUE / 10.0 END) as avg_max_temp,
                AVG(CASE WHEN o.ELEMENT = 'TMIN' THEN o.DATA_VALUE / 10.0 END) as avg_min_temp,
                MAX(CASE WHEN o.ELEMENT = 'TMAX' THEN o.DATA_VALUE / 10.0 END) as highest_temp,
                MIN(CASE WHEN o.ELEMENT = 'TMIN' THEN o.DATA_VALUE / 10.0 END) as lowest_temp,
                AVG(CASE WHEN o.ELEMENT = 'PRCP' THEN o.DATA_VALUE / 10.0 END) as avg_precipitation,
                SUM(CASE WHEN o.ELEMENT = 'SNOW' AND o.DATA_VALUE > 0 THEN 1 ELSE 0 END) as stations_with_snow
            FROM RAW_DAILY_OBSERVATIONS o
            WHERE o.DATE = %s
        """, (date,))

        row = cur.fetchone()

        summary = {
            'date': date,
            'formatted_date': f"{date[0:4]}-{date[4:6]}-{date[6:8]}",
            'station_count': row[0] or 0,
            'observation_count': row[1] or 0,
            'avg_max_temp_c': round(row[2], 1) if row[2] else None,
            'avg_min_temp_c': round(row[3], 1) if row[3] else None,
            'highest_temp_c': round(row[4], 1) if row[4] else None,
            'lowest_temp_c': round(row[5], 1) if row[5] else None,
            'avg_precipitation_mm': round(row[6], 1) if row[6] else None,
            'stations_with_snow': row[7] or 0
        }

        # Convert to Fahrenheit as well
        if summary['avg_max_temp_c'] is not None:
            summary['avg_max_temp_f'] = round(summary['avg_max_temp_c'] * 9/5 + 32, 1)
        if summary['avg_min_temp_c'] is not None:
            summary['avg_min_temp_f'] = round(summary['avg_min_temp_c'] * 9/5 + 32, 1)
        if summary['highest_temp_c'] is not None:
            summary['highest_temp_f'] = round(summary['highest_temp_c'] * 9/5 + 32, 1)
        if summary['lowest_temp_c'] is not None:
            summary['lowest_temp_f'] = round(summary['lowest_temp_c'] * 9/5 + 32, 1)

        cur.close()
        conn.close()
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/time-machine/<date>/extremes')
def get_extremes_on_date(date):
    """Get extreme weather locations for a specific date"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Get hottest location
        cur.execute("""
            SELECT
                o.ID,
                s.STATION_NAME,
                c.COUNTRY_NAME,
                o.DATA_VALUE / 10.0 as temp_c
            FROM RAW_DAILY_OBSERVATIONS o
            JOIN RAW_STATIONS s ON o.ID = s.STATION_ID
            JOIN RAW_COUNTRIES c ON LEFT(o.ID, 2) = c.COUNTRY_CODE
            WHERE o.DATE = %s AND o.ELEMENT = 'TMAX'
            ORDER BY o.DATA_VALUE DESC
            LIMIT 5
        """, (date,))

        hottest = [{'station': row[1], 'country': row[2], 'temp_c': row[3],
                    'temp_f': round(row[3] * 9/5 + 32, 1)} for row in cur.fetchall()]

        # Get coldest location
        cur.execute("""
            SELECT
                o.ID,
                s.STATION_NAME,
                c.COUNTRY_NAME,
                o.DATA_VALUE / 10.0 as temp_c
            FROM RAW_DAILY_OBSERVATIONS o
            JOIN RAW_STATIONS s ON o.ID = s.STATION_ID
            JOIN RAW_COUNTRIES c ON LEFT(o.ID, 2) = c.COUNTRY_CODE
            WHERE o.DATE = %s AND o.ELEMENT = 'TMIN'
            ORDER BY o.DATA_VALUE ASC
            LIMIT 5
        """, (date,))

        coldest = [{'station': row[1], 'country': row[2], 'temp_c': row[3],
                    'temp_f': round(row[3] * 9/5 + 32, 1)} for row in cur.fetchall()]

        # Get wettest location
        cur.execute("""
            SELECT
                o.ID,
                s.STATION_NAME,
                c.COUNTRY_NAME,
                o.DATA_VALUE / 10.0 as precip_mm
            FROM RAW_DAILY_OBSERVATIONS o
            JOIN RAW_STATIONS s ON o.ID = s.STATION_ID
            JOIN RAW_COUNTRIES c ON LEFT(o.ID, 2) = c.COUNTRY_CODE
            WHERE o.DATE = %s AND o.ELEMENT = 'PRCP' AND o.DATA_VALUE > 0
            ORDER BY o.DATA_VALUE DESC
            LIMIT 5
        """, (date,))

        wettest = [{'station': row[1], 'country': row[2], 'precip_mm': row[3]} for row in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({
            'hottest': hottest,
            'coldest': coldest,
            'wettest': wettest
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/time-machine/<date>/countries')
def get_countries_on_date(date):
    """Get weather by country for a specific date"""
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                c.COUNTRY_NAME,
                COUNT(DISTINCT o.ID) as stations,
                AVG(CASE WHEN o.ELEMENT = 'TMAX' THEN o.DATA_VALUE / 10.0 END) as avg_max,
                AVG(CASE WHEN o.ELEMENT = 'TMIN' THEN o.DATA_VALUE / 10.0 END) as avg_min
            FROM RAW_DAILY_OBSERVATIONS o
            JOIN RAW_COUNTRIES c ON LEFT(o.ID, 2) = c.COUNTRY_CODE
            WHERE o.DATE = %s
            GROUP BY c.COUNTRY_NAME
            HAVING COUNT(DISTINCT o.ID) >= 5
            ORDER BY stations DESC
            LIMIT 20
        """, (date,))

        countries = [{
            'country': row[0],
            'stations': row[1],
            'avg_max_c': round(row[2], 1) if row[2] else None,
            'avg_min_c': round(row[3], 1) if row[3] else None
        } for row in cur.fetchall()]

        cur.close()
        conn.close()
        return jsonify(countries)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
