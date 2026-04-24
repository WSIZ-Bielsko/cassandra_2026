def insert_st(session):
    return session.prepare(
        "INSERT INTO files (file_id, author_id, filename, created_at, content) "
        "VALUES (?, ?, ?, ?, ?)"
    )

def get_by_id_st(session):
    return session.prepare("SELECT * FROM files WHERE file_id = ?")


def get_by_author_st(session):
    return session.prepare("SELECT file_id, author_id, created_at, filename FROM files WHERE author_id = ?")


def get_by_time_range_st(session):
    return session.prepare("SELECT * FROM files WHERE created_at >= ? AND created_at <= ?")
