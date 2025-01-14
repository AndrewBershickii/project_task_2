/* п.1 Подготовить запрос, который определит корректное значение поля account_in_sum.
 * Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются,
 * то корректным выбирается значение account_out_sum предыдущего дня.*/
SELECT account_rk,
	   effective_date,
	   LAG(account_out_sum, 1, 0) OVER(PARTITION BY account_rk ORDER BY effective_date) AS account_in_sum,
	   account_out_sum
  FROM account_balance ab;

/* п.2 Подготовить такой же запрос, только проблема теперь в том, что account_in_sum одного дня правильная,
 * а account_out_sum предыдущего дня некорректна. Это означает, что если эти значения отличаются,
 * то корректным значением для account_out_sum предыдущего дня выбирается значение account_in_sum текущего дня.*/
SELECT account_rk,
	   effective_date,
	   account_in_sum,
	   COALESCE(LEAD(account_in_sum, 1) OVER(PARTITION BY account_rk ORDER BY effective_date), account_out_sum) AS account_out_sum
 FROM account_balance ab; 

-- п.3 Подготовить запрос, который поправит данные в таблице rd.account_balance используя уже имеющийся запрос из п.1
WITH UpdatedValues AS (
	SELECT account_rk,
	   effective_date,
	   LAG(account_out_sum, 1, 0) OVER(PARTITION BY account_rk ORDER BY effective_date) AS account_in_sum,
	   account_out_sum
 FROM account_balance
)

UPDATE account_balance
   SET account_in_sum = (
	   SELECT uv.account_in_sum
		 FROM UpdatedValues uv
		WHERE uv.account_rk = account_balance.account_rk
		  AND uv.effective_date = account_balance.effective_date);

