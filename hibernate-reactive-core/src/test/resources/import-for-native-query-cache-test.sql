-- This import is required for NativeQueryCacheMixedReturnTypeTest
ALTER TABLE test_user ADD COLUMN extra_col1 VARCHAR(50);
ALTER TABLE test_user ADD COLUMN extra_col2 VARCHAR(50);
INSERT INTO test_user (id, name, email, age, address, phone, extra_col1, extra_col2) VALUES (1, 'john', 'john@test.com', 30, 'ny', '123456', 'ext1', 'ext2');
