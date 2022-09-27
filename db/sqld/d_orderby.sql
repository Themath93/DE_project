-- *** order by ***
-- 결과를 정렬할 때 사용하는 구문
-- select문의 가장 마지막에 작성, 실행순서도 가장 마지막
-- 해석순서 : FROM, WHERE, GROUP BY, HAVING, SELECT, ORDER BY


-- ORDER BY 작성법
-- ORDER BY 컬럼명 ASC : 컬럼으로 오름차순 정렬, 정렬 기준인 컬럼이 하나 뿐이라면 생략 가능
-- ORDER BY 컬럼명 DESC : 컬럼으로 내림차순 정렬, 생략 불가
-- NULLS FIRST : 컬럼값에 NULL이 있을 경우 앞에 정렬
-- NULLS LAST : 컬럼값에 NULL이 있을 경우 뒤에 정렬

-- 사원의 이름, 급여, 부서코드, 보너스, 연봉레벨을 조회
select emp_name, salary, dept_code, bonus, sal_level
from employee
-- 이름으로 내침라순 정렬
-- order by emp_name desc;
-- 연봉으로 오름 차순
-- order by salary;
-- select절을 수행한 이후의 resultset에서의 두 번째 컬럼으로 내림차순 정렬
-- order by 2 desc; -- 가독성이 안좋아 쓰지 않음
-- 연봉레벨로 오름차순 정렬, 연봉레벨이 같다면 연봉을 기준으로 내림차순
-- order by sal_level ASC, salary DESC;
-- 보너스를 기준으로 오름차순, 컬럼값에 null이 있으면 null을 위쪽으로 정렬
-- order by bonus asc NULLS FIRST;
order by bonus asc NULLS LAST;




