
drop schema if exists data cascade;
create schema data;

set SEARCH_PATH to data;

create table roles (
    role_id varchar(64) primary key
);

create table permissions (
    role_id varchar(64) not null references roles,
    permission varchar(256) not null,
    primary key (role_id, permission)
);

create table users (
    id uuid primary key default uuidv7(),
    role_id varchar(64) not null references roles
);

create table admins (
    id uuid primary key references users
);

create table technicians (
    id uuid primary key references users,
    base_salary float8 not null
);

create table customers (
    id uuid primary key references users,
    name text not null,
    experience_since date not null,
    personal_info jsonb not null default '{}'
);

create table actions_log (
    id uuid primary key default uuidv7(),
    user_id uuid references users,
    action text not null,
    logged_at timestamptz not null default now(),
    details jsonb
);

create type sex as enum ('MALE', 'FEMALE');

create table droids (
    id uuid primary key default uuidv7(),
    sex sex not null,
    class text not null,
    capacity int not null,
    order_cost float8 not null,
    maintenance_cost float8 not null
);

create table products (
    id uuid primary key default uuidv7(),
    price float8 not null,
    name text not null
);

create table alcohol (
    product_id uuid primary key references products,
    brand text not null,
    kind text not null,
    volume float8 not null,
    quality float8 not null,
    strength float8 not null
);

create table snacks (
    product_id uuid primary key references products,
    kind text not null
);

create table conversation_topics (
    id uuid primary key default uuidv7(),
    name text unique not null,
    description text not null
);

create table promotions (
    id uuid primary key default uuidv7(),
    price float8 not null,
    name text not null,
    promotion_time tstzrange[] not null,
    available_time_hour int,
    avalible_user uuid references customers
);

create type order_status as enum ('SCHEDULED', 'IN_PROGRESS', 'COMPLETE', 'CANCELED', 'SUSPENDED');
create type order_type as enum ('COMMON', 'SPECIAL_MONTHLY_PROMO');

create table orders (
    id uuid primary key default uuidv7(),

    customer_id uuid not null references customers,
    promotion_id uuid references promotions,
    droid_id uuid not null references droids,

    status order_status not null,
    type order_type not null,

    droid_name text not null,
    address text not null
);

create table order_products (
    order_id uuid not null references orders,
    product_id uuid not null references products,
    amount int not null,
    primary key (order_id, product_id)
);

create table orders_topics (
    order_id uuid not null references orders,
    topic_id uuid not null references conversation_topics,
    primary key (order_id, topic_id)
);

create table snack_recommendations (
    id uuid primary key default uuidv7(),
    snack_id uuid not null references snacks,
    alcohol_id uuid references alcohol,
    topic_id uuid references conversation_topics,
    experience int4range,
    score int not null
);
create index snack_recommendations_alcohol_id_idx on snack_recommendations using btree(alcohol_id, score) where alcohol_id is not null;
create index snack_recommendations_topic_id_idx on snack_recommendations using btree(topic_id, score) where topic_id is not null;
create index snack_recommendations_experience_idx on snack_recommendations using gist(experience) where experience is not null;


create table stores (
    id uuid primary key default uuidv7(),
    address text not null,
    priority int not null,
    schedule jsonb not null
);

create table store_lots (
    product_id uuid not null references products,
    store_id uuid not null references stores,
    price float8 not null,
    in_stock int not null
);

create table promotion_products (
    promotion_id uuid not null references promotions,
    product_id uuid not null references products,
    amount int not null,
    primary key (promotion_id, product_id)
);

create table promotions_topics (
    promotion_id uuid not null references promotions,
    topic_id uuid not null references conversation_topics,
    primary key (promotion_id, topic_id)
);

create table service_log (
    id uuid primary key default uuidv7(),
    technician_id uuid not null references technicians,
    droid_id uuid not null references droids,
    serviced_at timestamptz not null,
    reward float8 not null
);
create index service_log_technician_id_idx on service_log(technician_id);
