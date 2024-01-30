--создаю схему

CREATE SCHEMA IF NOT exists DDM;

--табл 1
create table IF NOT exists DM_ACCOUNT_TURNOVER_F
(
    turn_date DATE NOT NULL,
    acct_num VARCHAR(20) NOT NULL,
    deb_turn_rub NUMERIC(16, 2),
    deb_turn_thous_rub NUMERIC(16, 2),
    cre_turn_rub NUMERIC(16, 2),
    cre_turn_thous_rub NUMERIC(16, 2),
    PRIMARY KEY (turn_date, acct_num)
)

--тут по части таблиц вообще нет пересечений в датах, т.е. получится, что да одни даты есть оборотная информация, 
--а на другие инфа по движению, ну да ладно

/*
select min(on_date), max(on_date)  from dds.ft_balance_f
select min(oper_date), max(oper_date)  from dds.ft_posting_f
select min(data_actual_date), max(data_actual_date), min(data_actual_end_date), max(data_actual_end_date)  from dds.md_account_d
select min(data_actual_date), max(data_actual_date), min(data_actual_end_date), max(data_actual_end_date)  from dds.md_currency_d
select min(data_actual_date), max(data_actual_date), min(data_actual_end_date), max(data_actual_end_date) from dds.md_exchange_rate_d
*/

--табл 2 (в правилах к 101 форме было про пустые значения для дебет/кредит оборотов с 2018 года, если имеется в виду null, а не 0, не ставлю ограничение not null)
--на остальное ставлю.
-- значения с т.р. с размерностью 16 указанной в файле сомнительны в том плане, что кол-во знаков после запятой, например 14(2), не указаны, поэтому туи (16, 0))
create table IF NOT exists DDM.DM_F101_ROUND_F
(
	reg_num NUMERIC(4) NOT NULL, --REGN
    plan CHAR(1) NOT NULL, --plan
    num_sc VARCHAR(5) NOT NULL, --num_sc
    char_type CHAR(1) NOT NULL, --a_p
    amn_rub_th_in NUMERIC(16, 0) NOT NULL, --vr
    amn_curr_in NUMERIC(16, 0) NOT NULL, --vv
    amn_total_rub_th_in NUMERIC(33, 4) NOT NULL, --vitg
    deb_turn_rub_th NUMERIC(16, 0) , --ora
    deb_turn_curr NUMERIC(16, 0) , --ova
    deb_turn_total_rub_th NUMERIC(33, 4) , --oitga
    cre_turn_rub_th NUMERIC(16, 0) , --orp
    cre_turn_curr NUMERIC(16, 0) , --ovp
    cre_turn_total_rub_th NUMERIC(33, 4) , --oitgp
    amn_rub_th_out NUMERIC(16, 0) NOT NULL, --ir
    amn_curr_out NUMERIC(16, 0) NOT NULL, --iv
    amn_total_rub_th_out NUMERIC(33, 4) NOT NULL, --iitg
    dt DATE NOT NULL, --dt
    inf_indr NUMERIC(1) NOT NULL, --priz
    PRIMARY KEY (reg_num, plan, num_sc, dt)
)

--Входящие остатки (в рублях/валют/итого)
--джойню балансы на дату с инфой о валютах, которая пригодится
with balance as (
 select
	 balance.on_date, 
	 balance.account_rk,
	 balance.currency_rk, 
	 balance_out, 
	 currency_code, 
	 code_iso_char, 
	 balance_out*coalesce(reduced_cource,1) as all_in_rub  
 from DDS.FT_BALANCE_F balance
 inner join dds.md_currency_d currency --считаю, что все валюты должны быть в справочнике (и тут они есть пока)
	 on balance.currency_rk = currency.currency_rk  --по номеру счета
	 and balance.on_date >=currency.data_actual_date  -- по актуальному периоду дат
	 and balance.on_date<=currency.data_actual_end_date
 left join dds.md_exchange_rate_d ex_rate --нет перевода из рублей в рубли, поэтому для рублей останутся null, и для рублей заменю на курс перевода 1
	 on balance.currency_rk = ex_rate.currency_rk 
	 and balance.on_date >=ex_rate.data_actual_date 
	 and balance.on_date<=ex_rate.data_actual_end_date
),
--выполню группировку по условию задачи. 
--переведу в тысячи (не знаю, что в исходных данных, рубли или тысячи, буду считать, что рубли) и в целое число, 
--т.к. вроде как в типах данных в усл задачи просто число размерности 16 указано, без указания кол-ва знаков после запятой

amn_in as ( 
select on_date, account_rk, (sum(all_in_rub)/1000)::int as amn_total_rub_th_in,
((sum(all_in_rub) filter (where code_iso_char = 'RUB'))/1000)::int as amn_rub_th_in,
((sum(all_in_rub) filter (where code_iso_char <> 'RUB'))/1000)::int as amn_curr_in
from balance
group by on_date, account_rk),


--разделю в движении кредит и дебет для удобства из одной табл на 2 пункта
--кредит
--аналогично джойню инфу по валютам из справочников
turn_cre as (
select 
post_cre.oper_date, 
post_cre.credit_account_rk,
post_cre.credit_amount,  
acc.char_type,
acc.currency_rk,
acc.currency_code,
coalesce (ex_rate.reduced_cource, 1) as reduced_cource,
post_cre.credit_amount * coalesce (ex_rate.reduced_cource, 1) as all_in_rub,
code_iso_char
from dds.ft_posting_f post_cre
inner join dds.md_account_d acc --для части счетов не нашлось информации в справочнике, т.к. тестовые данные, считаю, что соответствия должны быть и оставлю inner join
	on  acc.account_rk = post_cre.credit_account_rk 
	and post_cre.oper_date >= acc.data_actual_date 
	and post_cre.oper_date<=acc.data_actual_end_date
left join dds.md_exchange_rate_d ex_rate --нет перевода из рублей в рубли, поэтому для рублей останутся null
	on acc.currency_rk = ex_rate.currency_rk 
	and post_cre.oper_date >=ex_rate.data_actual_date 
	and post_cre.oper_date<=ex_rate.data_actual_end_date
inner join dds.md_currency_d currency --считаю, что все валюты должны быть в справочнике (тут все найдено)
	 on acc.currency_rk = currency.currency_rk 
	 and post_cre.oper_date >=currency.data_actual_date 
	 and post_cre.oper_date<=currency.data_actual_end_date),
	 
--дебет, все аналогично, только из мсходной табл с движением выбираю дебет столбцы
turn_deb as (
select 
post_deb.oper_date, 
post_deb.debet_account_rk,
post_deb.debet_amount,  
acc.char_type,
acc.currency_rk,
acc.currency_code,
coalesce (ex_rate.reduced_cource, 1) as reduced_cource,
post_deb.debet_amount * coalesce (ex_rate.reduced_cource, 1) as all_in_rub,
code_iso_char
--coalesce(reduced_cource,1) as course
from dds.ft_posting_f post_deb
inner join dds.md_account_d acc --для части счетов не нашлось информации в справочнике, т.к. тестовые данные, считаю, что соответствия должны быть и оставлю inner join
	on  acc.account_rk = post_deb.debet_account_rk 
	and post_deb.oper_date >= acc.data_actual_date 
	and post_deb.oper_date<=acc.data_actual_end_date
left join dds.md_exchange_rate_d ex_rate --нет перевода из рублей в рубли, поэтому для рублей останутся null
	on acc.currency_rk = ex_rate.currency_rk 
	and post_deb.oper_date >=ex_rate.data_actual_date 
	and post_deb.oper_date<=ex_rate.data_actual_end_date
inner join dds.md_currency_d currency 
	 on acc.currency_rk = currency.currency_rk 
	 and post_deb.oper_date >=currency.data_actual_date 
	 and post_deb.oper_date<=currency.data_actual_end_date), 
-- группирую по дате и считаю руб, валюта(переведенная в руб), итого
turn_cre_res as ( 
select oper_date, credit_account_rk, (sum(all_in_rub)/1000)::int as cre_turn_total_rub_th,
((sum(all_in_rub) filter (where code_iso_char = 'RUB'))/1000)::int as cre_turn_rub_th,
((sum(all_in_rub) filter (where code_iso_char <> 'RUB'))/1000)::int as cre_turn_curr
from turn_cre
group by oper_date, credit_account_rk),
--аналогично	 
turn_deb_res as ( 
select oper_date, debet_account_rk, (sum(all_in_rub)/1000)::int as deb_turn_total_rub_th,
((sum(all_in_rub) filter (where code_iso_char = 'RUB'))/1000)::int as deb_turn_rub_th,
((sum(all_in_rub) filter (where code_iso_char <> 'RUB'))/1000)::int as deb_turn_curr
from turn_deb
group by oper_date, debet_account_rk),

--соберу таблицу из присутствующих дат и счетов
all_dates as(

SELECT DISTINCT on_date AS dt, account_rk AS acc FROM amn_in
UNION
SELECT DISTINCT oper_date AS dt, credit_account_rk AS acc FROM turn_cre_res
UNION
SELECT DISTINCT oper_date AS dt, debet_account_rk AS acc FROM turn_deb_res
),


-- т.к. по инструкции формы 101 пустыми полями (буду считать что пустой = null) могут быть только кред и деб обороты, соберу общую табл (остатки + обороты) 
-- по-хорошему должны быть и остатки и обороты на одну дату, но сджойню, что есть
-- и заменю null на 0, как будто не пустые поля)
--за основу беру получившуюся табл дата + счет в прошлой цте
turn_plus_balance as(
select 
dt,
acc,
COALESCE(amn_in.amn_total_rub_th_in, 0) as amn_total_rub_th_in, 
COALESCE(amn_in.amn_rub_th_in, 0)AS amn_rub_th_in, 
COALESCE(amn_in.amn_curr_in, 0) AS amn_curr_in,
COALESCE(turn_cre_res.cre_turn_total_rub_th, 0) AS cre_turn_total_rub_th, 
COALESCE(turn_cre_res.cre_turn_rub_th, 0) AS cre_turn_rub_th, 
COALESCE(turn_cre_res.cre_turn_curr, 0) AS cre_turn_curr,
COALESCE(turn_deb_res.deb_turn_total_rub_th, 0) as deb_turn_total_rub_th, 
COALESCE(turn_deb_res.deb_turn_rub_th, 0) AS deb_turn_rub_th, 
COALESCE(turn_deb_res.deb_turn_curr, 0) AS deb_turn_curr,
acc.currency_code, 
acc.char_type
FROM all_dates
LEFT join amn_in ON amn_in.on_date = all_dates.dt and amn_in.account_rk = all_dates.acc
left join turn_cre_res on  all_dates.dt = turn_cre_res.oper_date and all_dates.acc = turn_cre_res.credit_account_rk
left join turn_deb_res on  all_dates.dt = turn_deb_res.oper_date and all_dates.acc = turn_deb_res.debet_account_rk
left join dds.md_account_d acc on  --и добавлю инфу с кодов валюты и типом счета опять для след пункта
all_dates.dt <=acc.data_actual_end_date and
all_dates.dt >=acc.data_actual_date and
all_dates.acc=acc.account_rk
-- данные не сильно подходящие, ничего не соединилось по сути с куча нулей). 
--дальше просто по условиям из задачи

)

select dt as on_date, 
acc as account_rk, 
currency_code, 
char_type, 
amn_total_rub_th_in, amn_rub_th_in, amn_curr_in, 
cre_turn_total_rub_th, cre_turn_rub_th, cre_turn_curr, 
deb_turn_total_rub_th, deb_turn_rub_th, deb_turn_curr,
coalesce(case 
	when char_type = 'A' and (currency_code = '643' or currency_code = '810' ) then amn_rub_th_in-cre_turn_rub_th+deb_turn_rub_th
	when char_type = 'P' and (currency_code = '643' or currency_code = '810' ) then amn_rub_th_in+cre_turn_rub_th-deb_turn_rub_th
end, 0) as amn_rub_th_out,
coalesce(case 
	when char_type = 'A' and not(currency_code = '643' or currency_code = '810' ) then amn_curr_in-cre_turn_curr+deb_turn_curr
	when char_type = 'P' and not(currency_code = '643' or currency_code = '810' ) then amn_curr_in+cre_turn_curr-deb_turn_curr
end, 0) as amn_curr_out,

coalesce(case 
	when char_type = 'A' and (currency_code = '643' or currency_code = '810' ) then amn_rub_th_in-cre_turn_rub_th+deb_turn_rub_th
	when char_type = 'P' and (currency_code = '643' or currency_code = '810' ) then amn_rub_th_in+cre_turn_rub_th-deb_turn_rub_th
end
+
case 
	when char_type = 'A' and not(currency_code = '643' or currency_code = '810' ) then amn_curr_in-cre_turn_curr+deb_turn_curr
	when char_type = 'P' and not(currency_code = '643' or currency_code = '810' ) then amn_curr_in+cre_turn_curr-deb_turn_curr
end , 0) as amn_total_rub_th_out


from turn_plus_balance
where currency_code is not null and char_type is not null --т.к не для всех счетов+дат нашлось соответствие, то уберу, что не нашлось, т.к. по-идее не могут быть null
--ну и тоже поменяла null по полям с case, как будто не пустые))


