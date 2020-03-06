# RawDataVerifyTool
MiWorks의 데이터 발행 시 데이터 검증을 위해 사용.
- 본 파일은 기존 검증툴에서  추가 및 수정한 부분만을 정리함  

## 사용법

### 1. 실행

- 검증

$ python3 raw_data_verify_tool.py > output.txt

- 압축 및 서버 전송

$ python3 after_verify_tool.py >> output.txt


## 동작 로직 

### Step 1. 엑셀 정의서 컨버팅하여 생성한 json파일 오픈, 딕셔너리로 추출 

- 규격 변동시 json 수정 매뉴얼 및 컨버터  :  



### Step 2. 필드 개수 검증 - delimiter_def_check(), cnt_fields(fullpath, file_def_list)
- 기존 검증 툴 필드 수 검증 방식은 첫 로우 필드 수부터 이상 있을 경우 이슈 발생 
- 정의서 딕셔너리와 비교하는 방식으로 수정 


### Step 3. 필드 타입 검증 - field_type_check(), compare_type(fullpath, file_def_list)
- 정의서의 data type필드를 통해  데이터 해당 필드의 타입과 일치 여부 검사


### Step 4. 필수 필드 검증 - null_field_check()  
- 정의서 '필수 여부' 필드를 통해 각 데이터의 null 여부 검사 


### Step 5. ',' 딜리미터 이상 검증 및 수정 - extraname_field_check() , correct_extraname_field(line_num, line)
- /Data_POI/ExtraName.txt 별칭 필드 검증
- 별칭 필드 내 구분자인 ',' 의 오류 여부 검사 
- 오류 존재 시 correct_extraname_field에서 데이터 파일을  정상 형태로 수정 


### Step 6. 레코드 사이즈 검증 - check_record_size() 
- /Data_POI/Local/PoiData_all.txt 데이터 검증 
-  레코드 4kB, 대표명칭필드 9B 초과 여부 검증 

### Step 7. PoiSubData_Multi 의 좌표정보 필드 검증 - comma_semicolone_check
- 각 좌표쌍은 세미콜론(;)으로 구분
- x, y 좌표는 TW정규화좌표로 콤마(,)로 구분
- 각 딜리미터 이상 여부 검증 




