drop table if exists tempmembershiptotals

select				--pulls all CRM data into a temporary table and assigns category of 'Member', 'Fan', 'Fragment' to each based on internal nomenclature.
	distinct s.contactid ,
	s.sf_contactid ,
	s.sf_accountid ,
	s.firstname ,
	s.lastname ,
	s.email ,
	s.zip ,
	regexp_replace(s.homephone,'([^0-9])','') as homephone, 
	regexp_replace(s.mobilephone,'([^0-9])','') as mobilephone, 
	regexp_replace(s.workphone,'([^0-9])','') as workphone,
	se.koreps__marital_status__c ,
	se.koreps__gender__c ,
	se.koreps__number_of_kids__c ,
	cd.birth_date ,
	s.addressline1 ,
		case			when z.region is null then 'Unknown' else z.region
	end as region,
		case
			when (s.firstname is not null
			and s.lastname is not null
			and s.email is not null
			and s.zip is not null
			and coalesce(nullif(s.mobilephone,''), nullif(s.homephone,''), nullif(s.workphone,'')) is not null) then 'Member'
			when (s.firstname is not null
			and s.lastname is not null
			and (s.email is not null
			or coalesce(nullif(s.mobilephone,''), nullif(s.homephone,''), nullif(s.workphone,'')) is not null)) then 'Fan'
			when (s.email is not null
			or coalesce(nullif(s.mobilephone,''), nullif(s.homephone,''), nullif(s.workphone,'')) is not null) then 'Fragment'
			else null
	end as status,
--below creates a string of binary variables to indicate what demographic fields a given record has filled in - 'EZPFLABCGM', each standing for a differnt field, has been a useful way to categorize accounts for us
		case	when s.email is not null then 'E' else 'x'	end ||
		case	when s.zip is not null then 'Z'	else 'x'	end ||
		case	when coalesce(nullif(s.mobilephone,''), nullif(s.homephone,''), nullif(s.workphone,'')) is not null then 'P'	else 'x'	end ||
		case	when s.firstname is not null then 'F' else 'x'	end ||
		case	when s.lastname is not null then 'L' else 'x'	end ||
		case	when s.addressline1 is not null then 'A' else 'x'	end ||
		case	when cd.birth_date is not null then 'B' else 'x'	end ||
		case	when se.koreps__number_of_kids__c is not null then 'C' else 'x'	end ||
		case	when se.koreps__gender__c is not null then 'G' else 'x'	end ||
		case	when se.koreps__marital_status__c is not null then 'M' else 'x'
	end as EZPFLABCGM
into temporary table tempmembershiptotals
from
	korepss.syncedcontacts s
inner join korepss.syncedcontacts_extension se on
	se.sf_contactid = s.sf_contactid
left join mlsclubfeed.customer_dim cd on
	lower(cd.email_address) = lower(s.email)
left join custom.skczipcsv_kpi z on
	left(s.zip, 5) = z.zip
where
	status != 'Member' and (
	s.firstname is not null
	or s.lastname is not null
	or s.zip is not null
	or s.email is not null
	or (s.homephone is not null
		or s.workphone is not null
		or s.mobilephone is not null))
		
		
		
--the below checks other database sources to see where we can fill in additional points of data missing from our CRM system		
select distinct
	m.contactid ,
	m.sf_contactid,
	m.sf_accountid,
	coalesce(m.firstname, a.firstname, a2.firstname, c.firstname, p.first_name) as nfirstname,
	coalesce(m.lastname, a.lastname, a2.lastname, c.lastname, p.last_name) as nlastname,
	m.email as nemail,
	coalesce(nullif(m.zip,''), nullif(a.zip,''), nullif(a2.zip,''), nullif(c.zip,''), nullif(p.address_postal,'')) as nzip,
	regexp_replace(coalesce(nullif(m.mobilephone,''), nullif(a.cellphone,''), nullif(a2.cellphone,''), nullif(c.cellphone,'')),'([^0-9])','') as ncellphone,
	regexp_replace(coalesce(nullif(m.homephone,''), nullif(a.homephone,''), nullif(a2.homephone,''), nullif(c.homephone,''), nullif(a.otherphone,''), nullif(a2.otherphone,'')),'([^0-9])','') as nhomephone,
	regexp_replace(coalesce(nullif(m.workphone,''), nullif(a.workphone,''), nullif(a2.workphone,''), nullif(c.workphone,'')),'([^0-9])','') as nworkphone,
	coalesce(m.koreps__marital_status__c, a.marital_status, a2.marital_status) as nmarital,
	coalesce(nullif(m.koreps__gender__c,'0'), nullif(a.gender,'0') , nullif(a2.gender,'0'), nullif(p.gender,'0')) as ngender,
	m.koreps__number_of_kids__c as numkids,
	coalesce(m.birth_date, a.birthday, a2.birthday, cast(p.birth_date as timestamp)) as nbday,
	coalesce(m.addressline1, a.address01, a2.address01, c.addressline1, p.address_street_1) as naddress,
	m.region ,
	m.status ,
	m.ezpflabcgm,
		case	when nemail is not null and nemail != '' then 'E' else 'x'	end ||
		case	when nzip is not null and nzip != '' then 'Z'	else 'x'	end ||
		case	when coalesce(nullif(ncellphone,''), nullif(nhomephone,''), nullif(nworkphone,'')) is not null then 'P'	else 'x'	end ||
		case	when nfirstname is not null and nfirstname != '' and nfirstname != 'Empty' then 'F' else 'x'	end ||
		case	when nlastname is not null and nlastname != '' and nlastname != 'Empty' then 'L' else 'x'	end ||
		case	when naddress is not null and naddress != '' then 'A' else 'x'	end ||
		case	when nbday is not null then 'B' else 'x'	end ||
		case	when numkids is not null and numkids != '' then 'C' else 'x'	end ||
		case	when ngender is not null and ngender != '' and ngender != 0 then 'G' else 'x'	end ||
		case	when nmarital is not null and nmarital != '' then 'M' else 'x'
	end as nEZPFLABCGM
from
	tempmembershiptotals m
left join dwa.vwdim_account a 
	on m.email = a.emailaddress 
left join dwa.vwdim_account a2
	on m.email = a2.second_email 
left join insights.customer c 
	on m.email = c.email
left join yinzcam.profiles p 
	on m.email = p.email and p."data_source(s)" like 'Use%'
where m.ezpflabcgm != nEZPFLABCGM and status != 'Member' and nEZPFLABCGM like 'EZPFL%'
order by nemail
	
	
	
