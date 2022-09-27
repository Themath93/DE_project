-- *** Group by
-- 특정 컬럼을 기준으로 그룹을 만들어 준다.
-- group by의 조건절인 having 절에서 그룹함수를 사용할 수 있다.
-- group 함수 : sum, count, avg, max, min

-- group by 컬럼명...
-- 부서별 급여 총액 평균 급여를 확인
select dept_code, sum(salary) , avg(salary)
from employee
group by dept_code;

-- 여러 컬럼을 결합해 그룹을 만들기
-- 부서 내 직급별 급여의 합계와 평균을 구해보자
select dept_code, job_code, sum(salary), avg(salary)
from employee
group by dept_code, job_code
order by dept_code asc, job_code asc;

--1. 급여가 300만원 이상인 사원들의 부서별 급여 총액을 구해보자.
select dept_code ,sum(salary)
from employee
where salary >= 3000000
GROUP by dept_code;

--2. 급여의 평균의 300만원 이상인 부서를 조회
select dept_code, avg(salary)
from employee
group by dept_code
having avg(salary) >= 3000000;

--3. 부서 내 직급 내 연봉 레벨 별 급여의 최고 연봉과 최소 연봉을 구하시오
--  구한 뒤 부서를 기준으로 오름차순 정렬하고 NULL값은 아래에 정렬하시오
select dept_code, job_code, sal_level, max(salary*12), min(salary*12) 
from employee
group by dept_code, job_code, sal_level
order by dept_code asc, job_code asc, sal_level asc nulls last;


select * from employee
where dept_code ='D6' and job_code ='J3' and sal_level ='S4';




------------------------------------------------------------------------------
--집계함수(ROLLUP, CUBE)
--ROLLUP : 그룹별 중간 집계를 계산
--group by rollup(컬럼, 컬럼, 컬럼)
select dept_code, job_code, sal_level, sum(salary)
from employee
group by rollup (dept_code, job_code, sal_level)
order by dept_code asc, job_code asc, sal_level asc nulls last;


계
-- CURE
-- 그룹으로 지정된 모든 조합에 대해 집
select dept_code, job_code, sal_level, sum(salary)
from employee
group by cube (dept_code, job_code, sal_level)
order by dept_code asc, job_code asc, sal_level asc nulls last;
