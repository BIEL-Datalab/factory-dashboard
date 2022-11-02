def EnergyPlusDeliverFilter():
    def mapping_df_types(df):
        dtypedict = {}
        for i, j in zip(df.columns, df.dtypes):
            if "object" in str(j):
                dtypedict.update({i: NVARCHAR(length=50)})
            if "float" in str(j):
                dtypedict.update({i: Float(precision=2, asdecimal=True)})
            if "int" in str(j):
                dtypedict.update({i: Integer()})
        return dtypedict

    import cx_Oracle as cx
    import pandas as pd
    db=cx.connect('tableua_q','tableua_2022','192.168.26.182:1521/orcl')
    cursor=db.cursor()
    cursor.execute('select 出货日期 as 日期,厂名 from V_DELIVER group by 出货日期,厂别名称')
    deliver_filter=pd.DataFrame(cursor.fetchall(),columns=['日期','厂名'])
    cursor.close()
    db.close()

    import pymssql
    db=pymssql.connect('192.168.89.43','biel','BIEL03010666','db_product_energy_tableau')
    cursor=db.cursor()
    cursor.execute('select 日期,厂名 from energy_Tableau_IE group by 日期,厂名 ')
    energy_filter=pd.DataFrame(cursor.fetchall(),columns=['日期','厂名'])
    db.close()

    from sqlalchemy import create_engine
    energy_plus_deliver_filter=pd.concat(deliver_filter,energy_filter).drop_duplicates()
    conn = create_engine('mssql+pymssql://biel:BIEL03010666@192.168.89.43/db_product_energy_tableau')
    data_energy.to_sql(name='energy_plus_deliver_filter', con=conn, if_exists='replace', index=False,dtype=mapping_df_types(df))
    conn.dispose()

    import pymssql
    db = pymssql.connect('192.168.89.43','biel','BIEL03010666','db_product_energy_tableau')  # 服务器名,账户,密码,数据库名
    cursor = db.cursor()  # 创建一个游标对象,python里的sql语句都要通过cursor来执行
    cursor.execute(''' alter table energy_plus_deliver_filter alter column 日期 datetime''')
    db.commit()
    cursor.close() #关闭游标
    db.close()
    print('EnergyPlusDeliverFilter----Success!!!')