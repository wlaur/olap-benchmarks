CREATE TABLE neighbourhoods (
    neighbourhood_group text,
    neighbourhood bigint PRIMARY KEY
);

CREATE TABLE listings (
    id bigint PRIMARY KEY,
    name text,
    host_id bigint,
    host_name text,
    neighbourhood_group text,
    neighbourhood bigint REFERENCES neighbourhoods(neighbourhood),
    latitude double precision,
    longitude double precision,
    room_type text,
    price bigint,
    minimum_nights bigint,
    number_of_reviews bigint,
    last_review date,
    reviews_per_month double precision,
    calculated_host_listings_count bigint,
    availability_365 bigint,
    number_of_reviews_ltm bigint,
    license text
);

CREATE TABLE calendar (
    listing_id bigint REFERENCES listings(id),
    date date,
    available boolean,
    price text,
    adjusted_price text,
    minimum_nights bigint,
    maximum_nights bigint
);

CREATE TABLE reviews (
    listing_id bigint REFERENCES listings(id),
    date date
);

CREATE TABLE reviews_detailed (
    listing_id bigint REFERENCES listings(id),
    id bigint PRIMARY KEY,
    date date,
    reviewer_id bigint,
    reviewer_name text,
    comments text
);

CREATE TABLE listings_detailed (
    id bigint PRIMARY KEY REFERENCES listings(id),
    listing_url text,
    scrape_id bigint,
    last_scraped date,
    source text,
    name text,
    description text,
    neighborhood_overview text,
    picture_url text,
    host_id bigint,
    host_url text,
    host_name text,
    host_since date,
    host_location text,
    host_about text,
    host_response_time text,
    host_response_rate text,
    host_acceptance_rate text,
    host_is_superhost boolean,
    host_thumbnail_url text,
    host_picture_url text,
    host_neighbourhood text,
    host_listings_count bigint,
    host_total_listings_count bigint,
    host_verifications text,
    host_has_profile_pic boolean,
    host_identity_verified boolean,
    neighbourhood text,
    neighbourhood_cleansed bigint,
    neighbourhood_group_cleansed text,
    latitude double precision,
    longitude double precision,
    property_type text,
    room_type text,
    accommodates bigint,
    bathrooms text,
    bathrooms_text text,
    bedrooms bigint,
    beds bigint,
    amenities text,
    price text,
    minimum_nights bigint,
    maximum_nights bigint,
    minimum_minimum_nights bigint,
    maximum_minimum_nights bigint,
    minimum_maximum_nights bigint,
    maximum_maximum_nights bigint,
    minimum_nights_avg_ntm double precision,
    maximum_nights_avg_ntm double precision,
    calendar_updated date,
    has_availability boolean,
    availability_30 bigint,
    availability_60 bigint,
    availability_90 bigint,
    availability_365 bigint,
    calendar_last_scraped date,
    number_of_reviews bigint,
    number_of_reviews_ltm bigint,
    number_of_reviews_l30d bigint,
    first_review date,
    last_review date,
    review_scores_rating double precision,
    review_scores_accuracy double precision,
    review_scores_cleanliness double precision,
    review_scores_checkin double precision,
    review_scores_communication double precision,
    review_scores_location double precision,
    review_scores_value double precision,
    license text,
    instant_bookable boolean,
    calculated_host_listings_count bigint,
    calculated_host_listings_count_entire_homes bigint,
    calculated_host_listings_count_private_rooms bigint,
    calculated_host_listings_count_shared_rooms bigint,
    reviews_per_month double precision
);

CREATE INDEX idx_listings_neighbourhood ON listings (neighbourhood);

CREATE INDEX idx_listings_group ON listings (neighbourhood_group);

CREATE INDEX idx_listings_room_type ON listings (room_type);

CREATE INDEX idx_listings_host ON listings (host_id);

CREATE INDEX idx_listings_last_review ON listings (last_review);

CREATE INDEX idx_listings_price ON listings (price);

CREATE INDEX idx_listings_reviews_cnt ON listings (number_of_reviews);

CREATE INDEX idx_listings_nbhd_room ON listings (neighbourhood, room_type);

CREATE INDEX idx_calendar_listing_date ON calendar (listing_id, date);

CREATE INDEX idx_calendar_date ON calendar (date);

CREATE INDEX idx_calendar_available ON calendar (available);

CREATE INDEX idx_reviews_listing_date ON reviews (listing_id, date);

CREATE INDEX idx_reviews_date ON reviews (date);

CREATE INDEX idx_ld_host ON listings_detailed (host_id);

CREATE INDEX idx_ld_room_type ON listings_detailed (room_type);

CREATE INDEX idx_ld_property_type ON listings_detailed (property_type);

CREATE INDEX idx_ld_accommodates ON listings_detailed (accommodates);

CREATE INDEX idx_ld_last_review ON listings_detailed (last_review);

CREATE INDEX idx_ld_review_score_rating ON listings_detailed (review_scores_rating);
