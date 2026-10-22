CREATE TABLE aka_name (
    id INT NOT NULL,
    person_id INT NOT NULL,
    name VARCHAR(65533) NOT NULL,
    imdb_index VARCHAR(48) NULL,
    name_pcode_cf VARCHAR(20) NULL,
    name_pcode_nf VARCHAR(20) NULL,
    surname_pcode VARCHAR(20) NULL,
    md5sum VARCHAR(128) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE aka_title (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    title VARCHAR(65533) NOT NULL,
    imdb_index VARCHAR(48) NULL,
    kind_id INT NOT NULL,
    production_year INT NULL,
    phonetic_code VARCHAR(20) NULL,
    episode_of_id INT NULL,
    season_nr INT NULL,
    episode_nr INT NULL,
    note VARCHAR(65533) NULL,
    md5sum VARCHAR(128) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE cast_info (
    id INT NOT NULL,
    person_id INT NOT NULL,
    movie_id INT NOT NULL,
    person_role_id INT NULL,
    note VARCHAR(65533) NULL,
    nr_order INT NULL,
    role_id INT NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE char_name (
    id INT NOT NULL,
    name VARCHAR(65533) NOT NULL,
    imdb_index VARCHAR(48) NULL,
    imdb_id INT NULL,
    name_pcode_nf VARCHAR(20) NULL,
    surname_pcode VARCHAR(20) NULL,
    md5sum VARCHAR(128) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE comp_cast_type (
    id INT NOT NULL,
    kind VARCHAR(128) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE company_name (
    id INT NOT NULL,
    name VARCHAR(65533) NOT NULL,
    country_code VARCHAR(1020) NULL,
    imdb_id INT NULL,
    name_pcode_nf VARCHAR(20) NULL,
    name_pcode_sf VARCHAR(20) NULL,
    md5sum VARCHAR(128) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE company_type (
    id INT NOT NULL,
    kind VARCHAR(128) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE complete_cast (
    id INT NOT NULL,
    movie_id INT NULL,
    subject_id INT NOT NULL,
    status_id INT NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE info_type (
    id INT NOT NULL,
    info VARCHAR(128) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE keyword (
    id INT NOT NULL,
    keyword VARCHAR(65533) NOT NULL,
    phonetic_code VARCHAR(20) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE kind_type (
    id INT NOT NULL,
    kind VARCHAR(60) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE link_type (
    id INT NOT NULL,
    link VARCHAR(128) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE movie_companies (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    company_id INT NOT NULL,
    company_type_id INT NOT NULL,
    note VARCHAR(65533) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE movie_info (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    info_type_id INT NOT NULL,
    info VARCHAR(65533) NOT NULL,
    note VARCHAR(65533) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE movie_info_idx (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    info_type_id INT NOT NULL,
    info VARCHAR(65533) NOT NULL,
    note VARCHAR(65533) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE movie_keyword (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    keyword_id INT NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE movie_link (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    linked_movie_id INT NOT NULL,
    link_type_id INT NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE name (
    id INT NOT NULL,
    name VARCHAR(65533) NOT NULL,
    imdb_index VARCHAR(48) NULL,
    imdb_id INT NULL,
    gender VARCHAR(4) NULL,
    name_pcode_cf VARCHAR(20) NULL,
    name_pcode_nf VARCHAR(20) NULL,
    surname_pcode VARCHAR(20) NULL,
    md5sum VARCHAR(128) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE person_info (
    id INT NOT NULL,
    person_id INT NOT NULL,
    info_type_id INT NOT NULL,
    info VARCHAR(65533) NOT NULL,
    note VARCHAR(65533) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE role_type (
    id INT NOT NULL,
    role VARCHAR(128) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE title (
    id INT NOT NULL,
    title VARCHAR(65533) NOT NULL,
    imdb_index VARCHAR(48) NULL,
    kind_id INT NOT NULL,
    production_year INT NULL,
    imdb_id INT NULL,
    phonetic_code VARCHAR(20) NULL,
    episode_of_id INT NULL,
    season_nr INT NULL,
    episode_nr INT NULL,
    series_years VARCHAR(196) NULL,
    md5sum VARCHAR(128) NULL
) ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 4
PROPERTIES ("replication_num" = "1");
