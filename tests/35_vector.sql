-- Vector support test
DROP TABLE IF EXISTS embeddings;

-- Create table with vector column
CREATE TABLE embeddings (
    id SERIAL PRIMARY KEY,
    name TEXT,
    embedding vector(3)
);

-- Insert vectors
INSERT INTO embeddings (name, embedding) VALUES
    ('doc1', '[1.0, 2.0, 3.0]'),
    ('doc2', '[4.0, 5.0, 6.0]'),
    ('doc3', '[1.0, 0.0, 0.0]');

-- Test L2 distance
SELECT name, l2_distance(embedding, '[1.0, 2.0, 3.0]') AS distance
FROM embeddings
ORDER BY distance;

-- Test cosine distance
SELECT name, cosine_distance(embedding, '[1.0, 1.0, 1.0]') AS distance
FROM embeddings
ORDER BY distance;

-- Test inner product
SELECT name, inner_product(embedding, '[1.0, 1.0, 1.0]') AS product
FROM embeddings
ORDER BY product;

-- Test vector_dims
SELECT name, vector_dims(embedding) AS dims
FROM embeddings;

-- Test vector_norm
SELECT name, vector_norm(embedding) AS norm
FROM embeddings;

-- Test vector casting
SELECT CAST('[7.0, 8.0, 9.0]' AS vector) AS vec;

-- Test similarity search (find nearest neighbors)
SELECT name, cosine_distance(embedding, '[1.0, 2.0, 3.0]') AS similarity
FROM embeddings
ORDER BY similarity
LIMIT 2;

-- Test L2 distance for nearest neighbor
SELECT name, l2_distance(embedding, '[1.0, 0.5, 0.0]') AS distance
FROM embeddings
ORDER BY distance
LIMIT 1;

-- Clean up
DROP TABLE embeddings;
