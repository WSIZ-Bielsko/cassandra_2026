1. clean up

- proper repo
- methods returning less than full objects (files are 1GB!)
`select file_id, filename from files where author_id=11111111-1111-1111-1111-111111111111;`
- insert with TTL
`INSERT INTO table_name (id, data) VALUES (1, 'value') USING TTL 86400;`
- 