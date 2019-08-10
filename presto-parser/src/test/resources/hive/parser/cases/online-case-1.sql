SELECT country,status,
  round(sum(stuckTime * 60)/count(1),2) avg_stuckTime
FROM
    (
    SELECT day,k4 as uid,
    concat(decode(from_hex(to_hex(cast(bitwise_and(k5,992)/ 32 AS INT) + 65)), 'US-ASCII'),
    decode(from_hex(to_hex(cast(bitwise_and(k5,31) AS INT) + 65)), 'US-ASCII')) AS country,
    cast(bitwise_and(k4,2) as int) as status,
bitwise_and(cast(players.k83 as bigint),1023) AS stuckTime
    FROM bigolive.live_sdk_video_stats_event_simplification CROSS JOIN UNNEST(k11) AS players
    WHERE
    day >= '2019-08-02'
    AND players.k80 <> 4294967295
and cast(bitwise_and(k77,15) as int) = 3
    and cast (bitwise_and(k8,2) as int) = 0
    ) t1
 WHERE country in ('BD','PH','RU')
GROUP BY status,country