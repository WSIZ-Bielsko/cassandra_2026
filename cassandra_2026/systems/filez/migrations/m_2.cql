use filex;

create custom index files_filename_idx
    on files (filename)
    using 'StorageAttachedIndex';