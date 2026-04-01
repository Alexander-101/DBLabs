
-- price - цена продажи клиенту (выручка)
-- cost - цена закупки/расходы (прибыль)

create or replace view orders_costs as
with order_droids_cost as (
    select
        o.id as order_id,
        sum(d.order_cost) * extract(hours from (upper(o.ordered_for) - lower(o.ordered_for))) as price,
        sum(sl.reward) as cost
    from orders o
             left join order_droids od on o.id = od.order_id
             left join droids d on od.droid_id = d.id
             left join service_log sl on od.order_id = sl.order_id and od.droid_id = sl.droid_id
    group by o.id
),
order_items_cost as (
    select
        o.id as order_id,
        sum(op.sell_price * op.amount) as price,
        sum(op.purchase_price * op.amount) as cost,
        sum(op.sell_price * op.amount * case when a.product_id is null then 1 else 0 end) as alcohol_price
    from orders o
             left join order_products op on o.id = op.order_id
             left join alcohol a on op.product_id = a.product_id
    group by o.id
)
select
    o.*,
    case when type = 'SPECIAL_MONTHLY_PROMO' then 0 else
    coalesce(p.price,
             coalesce(odc.price, 0) + coalesce(oic.price, 0)
    ) end as price,
    coalesce(odc.cost, 0) + coalesce(oic.cost, 0) as cost,
    coalesce(oic.alcohol_price, 0) as alcohol_price,
    coalesce(oic.price, 0) as items_price
from orders o
         left join order_items_cost oic on oic.order_id = o.id
         left join order_droids_cost odc on odc.order_id = o.id
         left join promotions p on p.id = o.promotion_id;


















-- 1. Получить отчет о взаимосвязи тем разговора и возрастных групп клиентов
-- (18-25, 26-35, 36-45, 46-55, 56 и старше).
-- Отчёт должен содержать по одной строке для каждого сочетания возрастная группа – тема для разговора.
-- Каждая строка должна содержать информацию о среднем алко-стаже клиентов в ней,
-- общем числе заказов, относящихся к соответствующей группе и теме разговора,
-- среднее число заказов в месяц с этой темой разговора в месяц (???? за какой период ????),
-- дата последнего заказа в этой возрастной группе с этой темой.

with user_topic_age as (
    select
        ct.name as topic,
        lower(o.ordered_for) as order_date,
        extract('year' from lower(o.ordered_for)) - extract('year' from c.birth_date) as age,
        extract('year' from lower(o.ordered_for)) - extract('year' from c.experience_since) as exp
    from orders o
        join customers c on c.id = o.customer_id
        join orders_topics ot on ot.order_id = o.id
        join conversation_topics ct on ct.id = ot.topic_id
),
user_topic_age_group as (
    select
        uta.topic,
        uta.exp,
        uta.order_date,
        case
            when age < 18 then '<18'
            when age < 26 then '18-26'
            when age < 36 then '26-35'
            when age < 46 then '36-45'
            when age < 56 then '46-55'
            else '56+' end
        as age_group
    from user_topic_age uta
)
select
    topic,
    age_group,
    count(*) as cnt,
    avg(exp) as avg_exp,
    max(order_date) as last_order,
    0 as avg_orders_per_month -- since 10^9 года до н.э.
from user_topic_age_group utag
group by topic, age_group;


-- 2. Получить список клиентов, для которых были организованы специальные мероприятия.
-- Для каждого клиента предоставить следующую информацию:
-- ФИО;
-- алкостаж;
-- адресе проживания;
-- дата проведения специального мероприятия;
-- количестве участвовавших в нем андроидов,
-- а также полной сумме затрат компании на мероприятие.
-- Дополнительно предоставить сведения о прибыли компании от данного клиента за этот месяц
-- (общий доход за месяц - затраты на обслуживание андроидов и проведение мероприятия).
-- Результат отсортировать в порядке убывания прибыли.

with customer_promos as (
    select
        o.id as promo_order_id,
        o.customer_id,
        lower(ordered_for)::date as "date",
        date_trunc('month', lower(ordered_for)) as month,
        (select count(*) from order_droids where order_id = o.id) as droids_cout,
        o.cost,
        o.address
    from orders_costs o where type = 'SPECIAL_MONTHLY_PROMO'
)
select
    cp.promo_order_id,
    any_value(cp.customer_id) as customer_id,
    any_value(cp.date) as date,
    any_value(cp.droids_cout) as droids_cout,
    any_value(cp.cost) as cost,
    sum(o.price - o.cost) as total_month_profit,
    any_value(c.name) as name,
    any_value(cp.address) as address,
    now() - any_value(c.experience_since) as expirience
from customer_promos cp
    join orders_costs o on cp.customer_id = o.customer_id and cp.month = date_trunc('month', lower(ordered_for))
    join customers c on c.id = cp.customer_id
group by cp.promo_order_id
order by total_month_profit desc;



-- 3. Сформировать запрос для вывода информации по клиентам, данные которых необходимо передать в разработку
-- @#$ в рамках ежемесячной программы сотрудничества "Крыша".
-- Для этого нужно вывести топ 5 клиентов по убыванию выручки с них,
-- любимой темой которых является "политика",
-- при этом доля стоимости алкоголя от общей выручки меньше порогового показателя 10%.
-- Эти люди подозрительны (говорят о политике и мало пьют),
-- необходимо вывести их
-- ФИО,
-- возраст и адрес,
-- сумму выручки,
-- число заказов.
-- Также опционально в формате excel прикрепить к ежемесячному отчёту туда...

create or replace view susp_users as
with user_topic as (
    select
        c.id as uid,
        ct.id as topic,
        count(*) as cnt
    from orders o
             join customers c on c.id = o.customer_id
             join orders_topics ot on ot.order_id = o.id
             join conversation_topics ct on ct.id = ot.topic_id
    group by c.id, ct.id
),
user_fav_topic as (
    select distinct on (uid) uid, topic from user_topic order by uid, cnt
)
select
    uid,
    sum(price) - sum(cost) as profit
from user_fav_topic ufv
    join orders_costs oc on oc.customer_id = uid
where ufv.topic = (select id from conversation_topics where name = 'politic')
group by uid
having sum(alcohol_price) / nullif(sum(items_price), 0) <= 0.1;


select
    c.*, profit
from customers c
    join susp_users su on c.id = su.uid
order by profit
limit 5;


-- 4. Алко-андроид при частых встречах с подозрительными клиентами способен осознать себя и стать пробудившимся ИИ.
-- Такие случаи могут привести сервис к банАнгараству.
-- Необходимо вывести информацию об андроидах, встречавшихся с подозрительными клиентами.
-- (За подозрительных стоит принимать клиентов, описанных в задании к предыдущему запросу, их может быть больше 5).
-- Нужно вывести название и имя андроидов,
-- кол-во встреч с подозрительными клиентами в порядке убывания вышеупомянутого количества.

explain
select * from susp_users limit 10;

select distinct on (d.id) d.*
from orders o
    join order_droids od on o.id = od.order_id
    join droids d on od.droid_id = d.id
    join susp_users su on o.customer_id = su.uid;


-- 5. Получить отчет о количестве заказов в разрезе по временным промежуткам.
-- В качестве временных промежутков выбрать отрезки по 3 часа,
-- начиная с 00:00 – 02:59 заканчивая 21:00 – 23:59. В отчете должно быть 8 строк (по числу промежутков).
-- Отчет должен содержать следующие столбцы:
-- Временной отрезок;
-- общее число заказов;
-- % от общего числа;
-- среднее число заказов в день в этот отрезок; (???? за какой период ????)
-- общая сумма выручки по заказам;
-- средняя сумма выручки в этот временной промежуток;
-- самый популярный вид алкоголя в этот временной отрезок.

with orders_with_bins as (
    select
        floor(extract(hour from lower(o.ordered_for)) / 3)::int * 3 as bin,
        oc.price,
        oc.cost
    from orders o
        join orders_costs oc on oc.id = o.id
),
res0 as (
    select
        bin::text || '-' || (bin + 3)::text as bin,
        count(*) as cnt,
        sum(price - cost) as total_profit,
        0 as avg_total_profit_per_day_per_period
    from orders_with_bins
    group by bin
)
select r.*,
    r.cnt / (select sum(cnt) from res0) * 100 as cnt_percent
from res0 r;













