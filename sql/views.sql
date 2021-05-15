--VIEWS

select asset, substring(asset, '[A-Z]+') from B3Log.B3SignalLogger  
WHERE strike = 0
group by 1,2
order by 1,2
limit 100