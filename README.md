# toolkits

## weeklySQA.py

生成SQA周报
### 1. 需要在python3下使用
python3 -m pip install pymysql openpyxl 

### 2. 参数修改
需要修改四个参数:
* save_dir ： 输出报表存放目录
* template_file : 模板文件，主要是修改一下路径
* start_date : 统计起始日期(包含)
* end_date   : 统计结束日期（包含）
```
def main() :
    '''
    '''
    # 准备
    save_dir = 'D:\\test\\py3'
    template_file = save_dir + '\\TEMP-开发任务检查表.xlsx'
    start_date = date(2018,3,31).strftime('%Y%m%d')
    end_date = date(2018,4,6).strftime('%Y%m%d')
    
 ```
