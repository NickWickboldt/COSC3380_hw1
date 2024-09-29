SELECT EXISTS (
	SELECT 1
	FROM information_schema.tables
	WHERE table_schema = 'public'
	AND table_name = 't1'
);

SELECT EXISTS (
	SELECT 1
	FROM information_schema.tables
	WHERE table_schema = 'public'
	AND table_name = 't2'
);

SELECT EXISTS (
	SELECT 1
	FROM information_schema.tables
	WHERE table_schema = 'public'
	AND table_name = 't3'
);

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't1'
	AND column_name = 'k1';

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't1'
	AND column_name = 'k2';

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't1'
	AND column_name = 'a';

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't1'
	AND column_name = 'b';

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't2'
	AND column_name = 'k2';

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't2'
	AND column_name = 'c';

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't3'
	AND column_name = 'k3';

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't3'
	AND column_name = 'k2';

SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = 't3'
	AND column_name = 'd';

SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
	ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = 't1';

SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
	ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = 't2';

SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
	ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = 't3';

SELECT COUNT(*)
FROM t1 AS c
LEFT JOIN T2 AS p
ON c.k2 = p.k2
WHERE p.k2 IS NULL;

SELECT COUNT(*)
FROM t3 AS c
LEFT JOIN T2 AS p
ON c.k2 = p.k2
WHERE p.k2 IS NULL;

SELECT k2, COUNT(DISTINCT A)
FROM T1
GROUP BY k2
HAVING COUNT(DISTINCT A) > 1;

SELECT k2, COUNT(DISTINCT B)
FROM T1
GROUP BY k2
HAVING COUNT(DISTINCT B) > 1;

SELECT A, COUNT(DISTINCT B)
FROM T1
GROUP BY A
HAVING COUNT(DISTINCT B) > 1;

SELECT k2, COUNT(DISTINCT D)
FROM T3
GROUP BY k2
HAVING COUNT(DISTINCT D) > 1;

