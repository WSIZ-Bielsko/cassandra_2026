use filex;

create table files
(
    file_id    uuid primary key,
    author_id  uuid,
    content    blob,
    created_at timestamp,
    filename   text
);

create custom index files_author_id_idx
    on files (author_id)
    using 'StorageAttachedIndex';

create custom index files_created_at_idx
    on files (created_at)
    using 'StorageAttachedIndex';

