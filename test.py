import mysql.connector
from typing import List, Tuple

def execute_first_query(connection: mysql.connector.MySQLConnection, 
                       src_station_g: int, 
                       dst_station_g: int) -> List[Tuple[int, int]]:
    cursor = connection.cursor()
    
    query = """
    WITH line_table AS (
        SELECT s.line_cd, l.line_name
        FROM m_station s
        LEFT OUTER JOIN m_line l
        ON s.line_cd = l.line_cd
        WHERE station_g_cd IN (%s, %s)
        GROUP BY line_cd
        HAVING COUNT(station_name) = 2
    )
    SELECT st1.station_cd AS src_station_cd, st2.station_cd AS dst_station_cd
    FROM line_table
    JOIN (
        SELECT line_cd, station_cd, station_name
        FROM m_station
        WHERE station_g_cd = %s
    ) AS st1
    ON line_table.line_cd = st1.line_cd
    JOIN (
        SELECT line_cd, station_cd, station_name
        FROM m_station
        WHERE station_g_cd = %s
    ) AS st2
    ON line_table.line_cd = st2.line_cd;
    """
    
    cursor.execute(query, (src_station_g, dst_station_g, src_station_g, dst_station_g))
    results = cursor.fetchall()
    cursor.close()
    return results

def execute_recursive_query(connection: mysql.connector.MySQLConnection,
                          src_station: int,
                          dst_station: int) -> List[Tuple[int, int]]:
    cursor = connection.cursor()
    
    query = """
    WITH RECURSIVE via_stations_1 AS (
        -- 正方向の通過駅
        SELECT line_cd, station_cd1, station_cd2
        FROM m_station_join
        WHERE station_cd1 = %s
        UNION ALL
        SELECT next_st.line_cd, next_st.station_cd1, next_st.station_cd2
        FROM via_stations_1
        JOIN m_station_join AS next_st
        ON via_stations_1.station_cd2 = next_st.station_cd1
        WHERE via_stations_1.station_cd1 <> %s
    ),
    via_stations_2 AS (
        -- 負方向の通過駅
        SELECT line_cd, station_cd2, station_cd1
        FROM m_station_join
        WHERE station_cd2 = %s
        UNION ALL
        SELECT next_st.line_cd, next_st.station_cd2, next_st.station_cd1
        FROM via_stations_2
        JOIN m_station_join AS next_st
        ON via_stations_2.station_cd1 = next_st.station_cd2
        WHERE via_stations_2.station_cd2 <> %s
    ),
    via_stations_1_added AS (
        SELECT line_cd, station_cd1, station_cd2
        FROM via_stations_1
        UNION ALL
        SELECT MAX(line_cd), %s AS station_cd1, null AS station_cd2
        FROM via_stations_1
        WHERE NOT EXISTS(SELECT * FROM via_stations_1 WHERE station_cd1 = %s)
        AND EXISTS(SELECT * FROM via_stations_1 WHERE station_cd2 = %s)
        GROUP BY line_cd
    ),
    via_stations_2_added AS (
        SELECT line_cd, station_cd2, station_cd1
        FROM via_stations_2
        UNION ALL
        SELECT MAX(line_cd), %s AS station_cd2, null AS station_cd1
        FROM via_stations_2
        WHERE NOT EXISTS(SELECT * FROM via_stations_2 WHERE station_cd2 = %s)
        AND EXISTS(SELECT * FROM via_stations_2 WHERE station_cd1 = %s)
        GROUP BY line_cd
    ),
    via_stations_compared AS (
    SELECT line_cd, station_cd1 AS via_station_cd
    FROM via_stations_1_added
    WHERE EXISTS(SELECT 1 FROM via_stations_1_added WHERE station_cd1 = %s)
    UNION
    SELECT line_cd, station_cd2 AS via_station_cd
    FROM via_stations_2_added
    WHERE EXISTS(SELECT 1 FROM via_stations_2_added WHERE station_cd2 = %s)
    )
    SELECT m_line.line_name, m_station.station_name
    FROM via_stations_compared
    JOIN m_station
    ON via_stations_compared.via_station_cd = m_station.station_cd
    JOIN m_line
    ON via_stations_compared.line_cd = m_line.line_cd
    ;
    """
    
    cursor.execute(query, (src_station, dst_station, src_station, dst_station, dst_station, dst_station, dst_station, dst_station, dst_station, dst_station, dst_station, dst_station))
    results = cursor.fetchall()
    cursor.close()
    return results

def main():
    # データベース接続設定
    config = {
        'user': 'mysql',
        'password': 'mysql',
        'host': 'localhost',
        'database': 'ekidata'
    }
    
    try:
        connection = mysql.connector.connect(**config)
        
        # 最初のクエリを実行
        station_pairs = execute_first_query(connection, src_station_g=1130208, dst_station_g=1130105)
        
        # 各ペアに対して再帰的クエリを実行
        for src_station, dst_station in station_pairs:
            results = execute_recursive_query(connection, src_station, dst_station)
            print(f"経路結果 :", results)
            
    except mysql.connector.Error as err:
        print(f"エラーが発生しました: {err}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    main()
