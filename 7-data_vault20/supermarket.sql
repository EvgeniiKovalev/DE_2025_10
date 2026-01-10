--бронзовый слой
create schema if not exists stg;

--drop table stg.agg_sales
create table if not exists stg.agg_sales (
     ship_mode varchar(100)
    ,segment varchar(100)
    ,country varchar(100)
    ,city varchar(100)
    ,state varchar(100)
    ,postal_code  varchar(100)
    ,region varchar(100)
    ,category varchar(100)
    ,sub_category varchar(100)
    ,sales numeric(16,4)
    ,quantity integer
    ,discount numeric(4,2)
    ,profit numeric(16,4)
);

comment on table  stg.agg_sales is 'Агрегированные продажи в магазинах';
comment on column stg.agg_sales.ship_mode is 'Класс доставки'; 
comment on column stg.agg_sales.segment  is 'Сегмент покупателя'; 
comment on column stg.agg_sales.country  is 'Страна магазина'; 
comment on column stg.agg_sales.city  is 'Город магазина'; 
comment on column stg.agg_sales.state  is 'Штат магазина'; 
comment on column stg.agg_sales.postal_code  is 'Почтовый индекс магазина'; 
comment on column stg.agg_sales.region  is 'Регион магазина'; 
comment on column stg.agg_sales.category  is 'Категория товара'; 
comment on column stg.agg_sales.sub_category  is 'Подкатегория товара'; 
comment on column stg.agg_sales.sales  is 'Сумма продажи'; 
comment on column stg.agg_sales.quantity  is 'Количество товара'; 
comment on column stg.agg_sales.discount  is 'Коэффициент скидки'; 
comment on column stg.agg_sales.profit  is 'Прибыль/убыток продажи'; 

--  загружаем через импорт в dbeaver из csv файла, котороый предварительно скачиваем 
-- из https://www.kaggle.com/datasets/roopacalistus/superstore?select=SampleSuperstore.csv

--добавляем дату - период агрегации (усложнил задачу, предполагая что данные будут поступать ежемесячно, иногда будут перезагрузки в течении месяца)
alter table stg.agg_sales add report_period date;
comment on column stg.agg_sales.report_period  is 'Начало месяца за который агрегированы данные'; 

update stg.agg_sales set report_period = '2025-11-01'::date;
commit;

 

--серебряный слой
drop schema dds cascade;


create schema if not exists dds;
comment on schema dds is 'Детальный слой данных';

--1 хаб
--select * from dds.hub_segment
--drop table dds.hub_segment;
create table if not exists dds.hub_segment(
    segment varchar(100) not null,
    hk_segment bytea not null,
    s_load_dts timestamptz not null,
    s_load_source varchar(100) not null,
    constraint pk_hub_segment primary key(hk_segment),
    constraint uq_hub_segment_bk unique(segment)
);
comment on table dds.hub_segment is 'Сегменты покупателей';


--2 хаб
--drop table dds.hub_ship_mode;
create table if not exists dds.hub_ship_mode(
    ship_mode varchar(100) not null,
    hk_ship_mode bytea not null,
    s_load_dts timestamptz not null,
    s_load_source varchar(100) not null,
    constraint pk_hub_ship_mode primary key(hk_ship_mode),
    constraint uq_hub_ship_mode_bk unique(ship_mode)
);
comment on table dds.hub_ship_mode is 'Режимы доставки';

--3 хаб
--drop table dds.hub_sub_category;
create table if not exists dds.hub_product_category(
    category varchar(100) not null,
    sub_category varchar(100) not null,
    hk_product_category bytea not null,
    s_load_dts timestamptz not null,
    s_load_source varchar(100) not null,
    constraint pk_hub_product_category primary key(hk_product_category),
    constraint uq_hub_product_category_bk unique(category, sub_category)
);
comment on table dds.hub_product_category is 'Категории товаров';



--4 хаб
--drop table dds.hub_location;
create table if not exists dds.hub_location(
    country varchar(100) not null,
    region varchar(100) not null,
    state varchar(100) not null,
    city varchar(100) not null,    
    postal_code varchar(100) not null,
    hk_location bytea not null,
    s_load_dts timestamptz not null,
    s_load_source varchar(100) not null,
    constraint pk_hub_location primary key(hk_location),
    constraint uq_hub_location_bk unique(country, region, state, city, postal_code)
);
comment on table dds.hub_location is 'Локации';



-- 5 хаб
--drop table dds.hub_calendar;
create table if not exists dds.hub_calendar(
    report_period date not null, -- Первое число месяца
    hk_calendar bytea not null,
    s_load_dts timestamptz not null,
    s_load_source varchar(100) not null,
    CONSTRAINT pk_hub_calendar primary key(hk_calendar),
    CONSTRAINT uq_hub_calendar_bk unique(report_period)
);
comment on table dds.hub_calendar is 'Периоды агрегации';



-- 6 линк
--drop table dds.lnk_sales_agg;
create table if not exists dds.lnk_sales_agg(
    hk_lnk_sales_agg bytea not null, 
    hk_location bytea not null,
    hk_product_category bytea not null,
    hk_segment bytea not null,
    hk_ship_mode bytea not null,
    hk_calendar bytea not null,
    s_load_dts timestamptz not null,
    s_load_source varchar(100) not null,
    constraint pk_hub_lnk_sale_agg primary key (hk_lnk_sales_agg),
    constraint uq_hub_lnk_sale_agg_bk unique(hk_location, hk_product_category, hk_segment, hk_ship_mode, hk_calendar)
);
comment on table dds.lnk_sales_agg is 'Агрегированные продажи';

create index idx_lnk_sales_agg_location on dds.lnk_sales_agg(hk_location);
create index idx_lnk_sales_agg_product_category on dds.lnk_sales_agg(hk_product_category);
create index idx_lnk_sales_agg_segment on dds.lnk_sales_agg(hk_segment);
create index idx_lnk_sales_agg_ship_mode on dds.lnk_sales_agg(hk_ship_mode);
create index idx_lnk_sales_agg_calendar on dds.lnk_sales_agg(hk_calendar);



-- 7 сателлит
--drop table dds.sat_sales_agg_metrics;
create table if not exists dds.sat_sales_agg_metrics(
    sales numeric(16,4),
    quantity integer,
    discount numeric(4,2),
    profit numeric(16,4),
    hk_lnk_sales_agg bytea not null,
    hash_diff bytea not null,
    sub_id integer not null,
    valid_from timestamptz not null,
    valid_to timestamptz,
    is_active boolean not null default true,
    s_load_dts timestamptz not null,
    s_load_source varchar(100) not null,
    constraint pk_sat_sales_metrics primary key (hk_lnk_sales_agg, sub_id, valid_from)    
);

comment on table dds.sat_sales_agg_metrics is 
'Метрики продаж, сателлит типа Multi-Active Satellite, у которого несколько несколько активных строк для одного ключа ';
comment on column dds.sat_sales_agg_metrics.sub_id is 
'Идентификатор под-записи для поддержки Multi-Active Satellite (несколько активных строк на один ключ Линка)';

-- индекс для активных строк
create index idx_sat_sales_agg_metrics_active on dds.sat_sales_agg_metrics(hk_lnk_sales_agg, sub_id) where is_active = true;

-- индекс для поиска в истории (time travel)
create index idx_sat_sales_agg_metrics_history on dds.sat_sales_agg_metrics(hk_lnk_sales_agg, valid_from, valid_to);

--select * from dds.sat_sales_agg_metrics






---------------------------------------------------------------------------------------------------------------------
------ загрузка данных
begin transaction isolation level read committed;

insert into dds.hub_segment(segment, hk_segment, s_load_dts, s_load_source)
    select
        segment,
        decode(md5(segment), 'hex'),
        NOW(),
        'SRC1' 
    from stg.agg_sales
    group by segment
on conflict (hk_segment) do nothing; -- обеспечит идемпотентность(отсутствие дублей по хэшу линка hk_segment)
--commit;


insert into dds.hub_ship_mode(ship_mode, hk_ship_mode, s_load_dts, s_load_source)
    select 
        ship_mode,
        decode(md5(ship_mode), 'hex'),
        NOW(),
        'SRC1'
    from stg.agg_sales
    group by ship_mode
on conflict (hk_ship_mode) do nothing; -- обеспечит идемпотентность(отсутствие дублей по хэшу линка hk_segment)
--commit;

insert into dds.hub_product_category(
    category,
    sub_category,
    hk_product_category,
    s_load_dts,
    s_load_source
)
    select
        category,
        sub_category,
        decode(md5(category || '~' || sub_category), 'hex'),
        NOW(),
        'SRC1' 
    from stg.agg_sales
    group by category, sub_category
on conflict (hk_product_category) do nothing; -- обеспечит идемпотентность(отсутствие дублей по хэшу линка hk_product_category)
--commit;


--хаб
--truncate dds.hub_location
insert into dds.hub_location(country, region, state, city, postal_code, hk_location, s_load_dts, s_load_source)
    select 
        country, 
        region, 
        state, 
        city, 
        postal_code,
        decode(md5(country || '~' || region || '~' || state || '~' || city || '~' || postal_code), 'hex'),
        NOW(),
        'SRC1'
    from stg.agg_sales
    group by country, region, state, city, postal_code
on conflict (hk_location) do nothing; -- обеспечит идемпотентность(отсутствие дублей по хэшу линка hk_location)
--commit;



insert into dds.hub_calendar(report_period, hk_calendar, s_load_dts, s_load_source)
    select distinct
        report_period,
        decode(md5(to_char(report_period, 'YYYY-MM-DD')), 'hex'),
        NOW(),
        'SRC1'
    from stg.agg_sales
on conflict (hk_calendar) do nothing; -- обеспечит идемпотентность(отсутствие дублей по хэшу линка hk_calendar)
--commit;

--линк
insert into dds.lnk_sales_agg(
    hk_lnk_sales_agg,
    hk_location,
    hk_product_category,
    hk_segment,
    hk_ship_mode,
    hk_calendar,
    s_load_dts,
    s_load_source
)
select distinct
    decode(md5(
        md5(country || '~' || region || '~' || state || '~' || city || '~' || postal_code) || '~' ||
        md5(category    || '~' || sub_category) || '~' ||
        md5(segment)    || '~' ||
        md5(ship_mode)  || '~' ||
        md5(to_char(report_period, 'YYYY-MM-DD'))
    ), 'hex'),
    decode(md5(country || '~' || region || '~' || state || '~' || city || '~' || postal_code), 'hex'),
    decode(md5(category    || '~' || sub_category), 'hex'),
    decode(md5(segment), 'hex'),
    decode(md5(ship_mode), 'hex'),
    decode(md5(to_char(report_period, 'YYYY-MM-DD')), 'hex'),
    NOW(),
    'SRC1'    
from stg.agg_sales
on conflict (hk_lnk_sales_agg) do nothing; -- обеспечит идемпотентность(отсутствие дублей по хэшу линка hk_lnk_sales_agg)
--commit;


--truncate table dds.sat_sales_agg_metrics
--закрываем имеющиеся строки по ключу 
update dds.sat_sales_agg_metrics
set valid_to = now(), is_active = false
from stg.agg_sales
where hk_lnk_sales_agg  = 
        decode(md5(
            md5(country || '~' || region || '~' || state || '~' || city || '~' || postal_code) || '~' ||
            md5(category    || '~' || sub_category) || '~' ||
            md5(segment)    || '~' ||
            md5(ship_mode)  || '~' ||
            md5(to_char(report_period, 'YYYY-MM-DD'))), 'hex');


insert into dds.sat_sales_agg_metrics(
    sales,
    quantity,
    discount,
    profit,
    hk_lnk_sales_agg,
    hash_diff,
    sub_id,
    valid_from,
    valid_to,
    is_active,
    s_load_dts,
    s_load_source    
)
with src as(
    select
        sales,
        quantity,
        discount,
        profit,
        hk_lnk_sales_agg,
        hash_diff,
        row_number() over(partition by hk_lnk_sales_agg order by sales, quantity, discount, profit) as sub_id
    from (
        select 
            sales,
            quantity,
            discount,
            profit,
            decode(md5(
                md5(country || '~' || region || '~' || state || '~' || city || '~' || postal_code) || '~' ||
                md5(category    || '~' || sub_category) || '~' ||
                md5(segment)    || '~' ||
                md5(ship_mode)  || '~' ||
                md5(to_char(report_period, 'YYYY-MM-DD'))), 'hex') as hk_lnk_sales_agg,
            decode(md5(
                coalesce(sales::text,'')    || '~' ||
                coalesce(quantity::text,'') || '~' ||
                coalesce(discount::text,'') || '~' ||
                coalesce(profit::text,'')), 'hex') as hash_diff
        from stg.agg_sales
    ) s0
) 
select
    sales,
    quantity,
    discount,
    profit,
    hk_lnk_sales_agg,
    hash_diff,
    sub_id,
    now() as valid_from,
    '9999-12-31 23:59:59'::timestamptz as valid_to,
    true as is_active,
    NOW(),
    'SRC1'
from src 
where not exists(
    select null
    from dds.sat_sales_agg_metrics tgt
    where   
        0 = 0
        and tgt.sub_id = src.sub_id
        and tgt.hk_lnk_sales_agg = src.hk_lnk_sales_agg
        and tgt.hash_diff = src.hash_diff
        and tgt.s_load_dts = (select max(s_load_dts) from dds.sat_sales_agg_metrics where hk_lnk_sales_agg = src.hk_lnk_sales_agg)
);


------------- подход №1
---- в таблицах cdm храним значения хабов и сателлитов, для bi
drop schema cdm cascade;
create schema if not exists cdm;
commit;



create table if not exists cdm.profit(
    postal_code varchar(100),
    sub_category varchar(100),
    category varchar(100),
    segment varchar(100),
    ship_mode varchar(100),
    profit numeric(16,4),
    cost_price numeric(16,4),
    sales numeric(16,4),
    amount_discount numeric(16,4),
    report_period date
);
comment on table  cdm.profit is 'Агрегат выручки/прибыли-убытка/себестоимости во всех разрезах (для bi инструмента)';
comment on column cdm.profit.postal_code is 'почтовый индекс';
comment on column cdm.profit.category is 'категория товара';
comment on column cdm.profit.sub_category is 'подкатегория товара';
comment on column cdm.profit.segment is 'сегмент покупателя';
comment on column cdm.profit.ship_mode is 'класс доставки';
comment on column cdm.profit.sales is 'Выручка';
comment on column cdm.profit.profit is 'Прибыль (>0)/убыток (<0)';
comment on column cdm.profit.cost_price is 'Себестоимость';
comment on column cdm.profit.amount_discount is 'Сумма скидки';
comment on column cdm.profit.report_period is 'Первое число месяца, интервал агрегирования';

--truncate table cdm.profit;
insert into cdm.profit(
    report_period,
    postal_code,
    category,
    sub_category,
    segment,
    ship_mode,
    profit, 
    cost_price, 
    sales, 
    amount_discount
)
    select
        c.report_period,
        hs.postal_code,
        hsc.category,
        hsc.sub_category,
        s.segment,
        sd.ship_mode,
        sum(m.profit) as profit,
        sum(m.sales - m.profit) as cost_price,
        sum(m.sales) as sales,
        sum(m.sales/(1 - m.discount) - m.sales) as amount_discount
    from dds.lnk_sales_agg ag
        left join dds.hub_location hs on hs.hk_location = ag.hk_location
        left join dds.hub_segment s on s.hk_segment = ag.hk_segment
        left join dds.hub_product_category hsc on hsc.hk_product_category = ag.hk_product_category
        left join dds.hub_calendar c on c.hk_calendar = ag.hk_calendar
        left join dds.hub_ship_mode sd on sd.hk_ship_mode = ag.hk_ship_mode
        inner join dds.sat_sales_agg_metrics m on m.hk_lnk_sales_agg = ag.hk_lnk_sales_agg and m.is_active = true
    group by 
        c.report_period,
        hs.postal_code,
        hsc.category,
        hsc.sub_category,
        s.segment,
        sd.ship_mode;
commit;