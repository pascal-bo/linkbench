CREATE CONSTRAINT unique_node_id IF NOT EXISTS ON (n:node) ASSERT n.id IS UNIQUE;
CREATE INDEX link_type_index FOR ()-[l:link]-() ON (l.link_type);