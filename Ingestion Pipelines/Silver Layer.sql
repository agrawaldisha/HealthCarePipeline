-- departments ==> full load +cdm

-- If table is available truncate befire inserting

--creating full load department in silver layer
select distinct 
  concat (deptid,'-',datasource) as Dept_id,deptid as 
  src_dept_id , name, datasource,
  case
    when deptid is null or name is null then true
    else false
  end as is_quarantined
  from
    (select distinct * , 'hosa' as datasource from `gcpdataengineering-467713.bronze_dataset.departments_ha`
    union all
    select distinct * , 'hosb' as datasource from `gcpdataengineering-467713.bronze_dataset.departments_hb`
    )
