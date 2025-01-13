SELECT di.deal_rk,
	   di.effective_from_date,
	   di.effective_to_date,
	   di.agreement_rk,
	   di.account_rk,
	   di.client_rk,
	   di.department_rk,
	   di.product_rk,
	   p.product_name,
	   di.deal_type_cd,
	   di.deal_start_date,
	   di.deal_name,
	   di.deal_num,
	   di.deal_sum,
	   lhi.loan_holiday_type_cd,
	   lhi.loan_holiday_start_date,
	   lhi.loan_holiday_finish_date,
	   lhi.loan_holiday_fact_finish_date,
	   lhi.loan_holiday_finish_flg,
	   lhi.loan_holiday_last_possible_date
  FROM rd.deal_info di
	   LEFT JOIN rd.loan_holiday lhi ON di.deal_rk = lhi.deal_rk
	   			 AND di.effective_from_date = lhi.effective_from_date 
	   LEFT JOIN rd.product p on di.product_rk = p.product_rk
	   			 AND di.deal_name = p.product_name 
	   			 AND di.effective_from_date = p.effective_from_date;

