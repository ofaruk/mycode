SELECT *
FROM (
	SELECT g.group_name
		,g.all_test_cases
		,o.passed_test_cases
		,c.test_value
		,test_value * passed_test_cases total_value
	FROM (
		SELECT count(id) all_test_cases
			,group_name
		FROM test_cases
		GROUP BY group_name
		) AS g
	LEFT JOIN (
		SELECT group_name
			,count(id) passed_test_cases
		FROM test_cases
		WHERE STATUS = 'OK'
		GROUP BY group_name
		) AS o ON g.group_name = o.group_name
	LEFT JOIN (
		SELECT name
			,test_value
		FROM test_groups
		) c ON g.group_name = c.name
	) x
ORDER BY total_value ,group_name
