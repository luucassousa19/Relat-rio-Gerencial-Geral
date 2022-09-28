libname rmbarslt "/install/SASConfig/Lev1/AppData/SASIRM/pa/data/217954730/rmbarslt";
libname alpha "/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.sbrlus/input_area/07312019";
libname rmbastg "/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.sbrlus/landing_area/configurations/rmbalm5.1.2_eba_281_201906/rd_conf";
libname rmbabcnf "/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.sbrlus/input_area/configurations/rmbalm5.1.2_eba_281_201906/rmb_conf";
libname f_anbima "/install/SASConfig/Lev1/AppData/SASIRM/pa/data/217954730/rmbaparm";
/****************************************************************************************************/
%let in_holiday_lists = f_anbima.holiday_lists;

%include '/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.luterr/source/sas/ucmacros/functions/rsk_increment_workday.sas';
%include '/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.luterr/source/sas/ucmacros/functions/rsk_array_utilities.sas';
%include '/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.luterr/source/sas/ucmacros/functions/rsk_calc_next_workday.sas';
%include '/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.luterr/source/sas/ucmacros/functions/rsk_check_num_missing_pf.sas';
%include '/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.luterr/source/sas/ucmacros/functions/rsk_print_msg_subr.sas';
%include '/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.luterr/source/sas/ucmacros/functions/rsk_get_msg_subr.sas';
%include '/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.luterr/source/sas/ucmacros/functions/rsk_print_error_msg_and_abort.sas';
%include '/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.luterr/source/sas/ucmacros/functions/rsk_get_msg_subr2.sas';

proc fcmp outlib=work.myfuncs.dates;
    %rsk_array_utilities;
    %rsk_increment_workday;
    %rsk_calc_next_workday;
    %rsk_check_num_missing_pf;
    %rsk_print_msg_subr;
    %rsk_get_msg_subr;
    %rsk_print_error_msg_and_abort;
    %rsk_get_msg_subr2;
    subroutine criaArray (ArrayParam[*], sasDataset $, Column $);
    outargs ArrayParam;
    array ArrayTemp[1] / nosymbols;
    rc=read_array(sasDataset, ArrayTemp, Column);

   do i=1 to dim(ArrayTemp);
        ArrayParam[i]=ArrayTemp[i];
    end;
    endsub;
run;

quit;
options cmplib=work.myfuncs;

data BR_holidays (keep=ANBIMA);
    set &in_holiday_lists;
    call symputx("cxpn", _n_);
run;

/*****************************Código GRP19*************************/

	proc sql;
		create table allprice_imported as
		select t1.*,
			   t2.change_value,
/* 			   CASE when t2.change_type eq 'REL' then 'Multiplicativo' */
/* 			   		when t2.change_type eq 'ABS' then 'Aditivo' */
/* 					else '.' */
/* 			   end as FatorMA, */
			   CASE when t2.change_value < 0 then 'Down'
			        else 'Up'
			   end as stressType
		from rmbarslt.ALLPRICE as t1
		left join (select distinct scenario_name, change_value, change_type from rmbabcnf.nevscen_scenarios) as t2
		on t1.AnalysisName = t2.scenario_name;
				
	quit;
	
	/*-=-=-=-=-==-=-=-=-=-=-==--=-=-=- Tabela vazia-=-=-=-=-=-=-=-=-=-=-=-=*/
	data allprice_inicial;
		format  BaseDate NLDATE20.
				vertex 10.
				ResultName $32. 
				InstID $256.
				VALUE 8.
				PRIMITIVE_RF $32.
				x_br_cfdisc NLNUM16.2
				product_type $32. 
/* 				altype $32. */
/* 				FatorMA $32. */
				change_value 16.7;
		stop;
	run;
	
data nevscen_analysis_option;
set alpha.nevscen_analysis_option(where=(CONFIG_NAME = 'TIMEGRID'));
call symputx('CONFIG_VALUE',CONFIG_VALUE);
run;

proc sql noprint;
select max(time_bucket_seq_nbr) into :max from rmbastg.time_grid_bucket 
where TIME_GRID_ID="&CONFIG_VALUE.";
quit;
	
/* 	proc sql noprint; */
/* 		select max(time_bucket_seq_nbr) into :max. from rmbastg.time_grid_bucket */
/* 		where TIME_GRID_ID="&TIMEGRID."; */
/* 	quit; */
	
	%macro vertices;
	
		%let i=0;
		%do i = 1 %to &max.;
			
			proc sql noprint;
				select time_bucket_end_uom_no into: time_bucket_end_uom_no
				from rmbastg.time_grid_bucket
				where time_bucket_seq_nbr=&i and TIME_GRID_ID="&CONFIG_VALUE.";
			quit;
			
			data custom_ALLPRICE(keep= BaseDate ResultName vertex InstID PRIMITIVE_RF VALUE x_br_cfdisc product_type altype change_value);
 				set allprice_imported;
				vertex=&time_bucket_end_uom_no;
				x_br_cfdisc=X_BR_CFDISC_&i;
				rename AnalysisName = ResultName;
				rename _date_ = BaseDate;
			run;
			
			proc  append base=allprice_inicial  data=custom_ALLPRICE force nowarn;
			run;
		
		%end;
		
	%mend vertices;
	
	%vertices;
	
data ultima_tabela;
	set allprice_inicial;
	array HOLIDAY_LIST [&cxpn.] _temporary_;

	if _N_=1 then
		do;
			call criaArray(HOLIDAY_LIST, "work.BR_holidays", "ANBIMA");
		end;
		
	last_day_month=intnx('month', BaseDate, 0, 'e');	
	last_business_day_month = rsk_calc_next_workday("month", 0, "weekday", last_day_month, holiday_list, "MP", "P");
	format last_day_month last_business_day_month date10.;
	if last_business_day_month=BaseDate then flag_last_business_day_month=1; 
	else flag_last_business_day_month=0;
where strip(ResultName)  not like 'HISTORICO_%';
run;

