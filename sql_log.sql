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
	AND column_name = 'k3';

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
	AND column_name = 'd';

SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
	ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = 't1';

SELECT
  kcu.column_name AS fk_column,
  ccu.table_name AS referenced_table,
  ccu.column_name AS referenced_column
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_name = 't1';

SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
	ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = 't2';

SELECT
  kcu.column_name AS fk_column,
  ccu.table_name AS referenced_table,
  ccu.column_name AS referenced_column
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_name = 't2';

SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
	ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = 't3';

SELECT
  kcu.column_name AS fk_column,
  ccu.table_name AS referenced_table,
  ccu.column_name AS referenced_column
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_name = 't3';

SELECT COUNT(*)
FROM t1 AS c
LEFT JOIN t2 AS p
ON c.k2 = p.k2
WHERE p.k2 IS NULL;

SELECT COUNT(*)
FROM t2 AS c
LEFT JOIN t3 AS p
ON c.k3 = p.k3
WHERE p.k3 IS NULL;

