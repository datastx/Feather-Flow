SELECT
    a.id,
    a.a,
    b.b
FROM branch_a a
JOIN branch_b b ON a.id = b.id
