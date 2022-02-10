# pip install pymssql
# pip install pymysql
# pip install pandas
# pip install PyQt5
# QT Designer C:\Users\...\Anaconda3\Lib\site-packages\PySide2

import sys
import os
import pymysql
import pandas as pd
import datetime as d
import pymssql
import codecs
from PyQt5 import uic

from PyQt5.QtWidgets import (QProgressBar,QCheckBox ,QHBoxLayout, QDialog, QApplication, QMainWindow, QWidget, QGridLayout, QTableWidgetItem ,QLabel, QLineEdit, QTextEdit, QComboBox, QPushButton, QTableWidget, QVBoxLayout,QGridLayout,QInputDialog, QMessageBox)
from PyQt5 import QtCore

form_class = uic.loadUiType("./ui_MigMini_MS_TO_MY.ui")[0]

class StdoutRedirect(QtCore.QObject):
    # https://4uwingnet.tistory.com/9 참고함.
    printOccur = QtCore.pyqtSignal(str, str, name="print")

    def __init__(self, *param):
        QtCore.QObject.__init__(self, None)
        self.daemon = True
        self.sysstdout = sys.stdout.write
        self.sysstderr = sys.stderr.write

    def stop(self):
        sys.stdout.write = self.sysstdout
        sys.stderr.write = self.sysstderr

    def start(self):
        sys.stdout.write = self.write
        sys.stderr.write = lambda msg: self.write(msg, color="red")

    def write(self, s, color="black"):
        sys.stdout.flush()
        self.printOccur.emit(s, color)



class migMini_MS_TO_MY(QMainWindow, form_class):

    def __init__(self):
        self.get_fetch_size = 10000 # DB에서 한번에 읽어오는 건수
        QMainWindow.__init__(self)
        self.setupUi(self)
        self.initUI()

        # member variable
        self._stdout = StdoutRedirect()
        self._stdout.start()

        self.prgBar.setValue(0)
        self.dic_tabResHeader = {'CHK':0
            ,'S_TAB_CAT':1
            ,'S_TAB_SCH':2
            ,'S_TAB_NM':3
            ,'S_TAB_CNT':4
            ,'T_TAB_SCH':5
            ,'T_TAB_NM':6
            ,'T_TAB_CNT':7
            ,'MIG_RES':8}
        self.sql_getTableList_MSSQL = """SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES T1 WHERE T1.TABLE_CATALOG = '{0}' AND T1.TABLE_TYPE = 'BASE TABLE' AND T1.TABLE_NAME LIKE '%'+'{1}'+'%'"""
        self.sql_getDataCount_MSSQL = """SELECT COUNT(*) CNT FROM {0}.{1}.{2}"""
        self.sql_getDataCount_MySQL = """SELECT COUNT(*) CNT FROM {0}.{1}"""


    def SetFontUI(self):
        pass

    def OffMigBtn(self):
        self.btnMakeDDL.setEnabled(False)
        self.btnTabMapping.setEnabled(False)
        #self.btnTabMappingCountCheck.setEnabled(False)
        self.btnMigStart.setEnabled(False)
        self.btnSourMapCheck.setEnabled(False)
        self.btnSourAllCheck.setEnabled(False)
        self.btnSourAllUnCheck.setEnabled(False)
        self.btnNoSourMapCheck.setEnabled(False)
        self.btnSaveTab.setEnabled(False)

    def OnCheckBtn(self):
        self.btnSourMapCheck.setEnabled(True)
        self.btnSourAllCheck.setEnabled(True)
        self.btnSourAllUnCheck.setEnabled(True)
        self.btnNoSourMapCheck.setEnabled(True)
        self.btnSaveTab.setEnabled(True)

    def OnMigBtn(self):
        if self.targConnState == 1 and self.sourConnState == 1:
            self.btnMakeDDL.setEnabled(True)
            self.btnTabMapping.setEnabled(True)
            #self.btnTabMappingCountCheck.setEnabled(True)
            self.btnMigStart.setEnabled(True)


    def initUI(self):
        self.targConnState = 0
        self.sourConnState = 0

        self.txtLog.setReadOnly(True)
        self.btnSourConnect.clicked.connect(self.btnSourConnectClicked)
        self.btnTargConnect.clicked.connect(self.btnTargConnectClicked)

        self.txtSourHost.setText('')
        self.txtSourPort.setText('')
        self.txtSourUserID.setText('')
        self.txtSourPwd.setText('')
        self.txtSourDBName.setText('')
        self.txtTargHost.setText('')
        self.txtTargPort.setText('')
        self.txtTargUserID.setText('')
        self.txtTargPwd.setText('')
        self.txtTargDBName.setText('')

        self.txtSourPwd.setEchoMode(QLineEdit.Password)
        self.txtTargPwd.setEchoMode(QLineEdit.Password)

        self.btnSourAllCheck.clicked.connect(self.btnSourAllCheckClicked)
        self.btnSourAllUnCheck.clicked.connect(self.btnSourAllUnCheckClicked)
        self.btnSourMapCheck.clicked.connect(self.btnSourMapCheckClicked)
        self.btnNoSourMapCheck.clicked.connect(self.btnNoSourMapCheckClicked)


        self.btnMakeDDL.clicked.connect(self.btnMakeDDLClicked)
        self.btnTabMapping.clicked.connect(self.btnTabMappingClicked)
        self.btnSaveTab.clicked.connect(self.btnSaveTabClicked)

        #self.btnTabMappingCountCheck = QPushButton(self)
        #self.btnTabMappingCountCheck.setText('소스 타겟 건수 체크')
        #self.btnTabMappingCountCheck.clicked.connect(self.btnTabMappingCountCheckClicked)
        #self.layoutH_btns.addWidget(self.btnTabMappingCountCheck)


        self.btnMigStart.clicked.connect(self.btnMigStartClicked)


        #self.tabSour.setSelectionMode(QtCore.Qt.QAbstractItemView.SingleSelection)
        self.tabSour.setAlternatingRowColors(True)
        self.tabSour.setSelectionMode(QTableWidget.SingleSelection)
        self.tabSour.setSelectionBehavior(QTableWidget.SelectRows)

        #self.setLayout(self.vlay_Main)
        self.OffMigBtn()
        self.SetFontUI()
        self.show()


    def printLog(self,str):
        self.txtLog.insertPlainText("\n\n=========================================\n")
        self.txtLog.insertPlainText(d.datetime.now().strftime("%Y-%m-%d, %H:%M:%S") + '\n')
        self.txtLog.insertPlainText(str)
        self.txtLog.insertPlainText("\n=========================================\n\n")
        self.txtLog.ensureCursorVisible()
        QApplication.processEvents(QtCore.QEventLoop.ExcludeUserInputEvents)

    def chkConnect(self):

        if self.targConnState != 1:
            #QMessageBox.warning(self, "Error", "타겟DB 로그인이 올바르지 않습니다.")
            self.printLog("타겟DB 로그인이 올바르지 않습니다.")
            return -1
        if self.sourConnState != 1:
            #QMessageBox.warning(self, "Error", "소스DB 로그인이 올바르지 않습니다.")
            self.printLog("소스DB 로그인이 올바르지 않습니다.")
            return -1

        return 1

    def btnTabMappingClicked(self):
        if self.chkConnect() != 1:
            return

        try:
            # 2022.01.27 change start
            if self.txtMarSrchIn.text() != '':
                self.getSourceTableListUseIn(self.txtMarSrchIn.text())
            else:
                self.getSourceTableList(self.txtMapSrchWord.text())
            # 2022.01.27 change fin

            sql_getTargTabList = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES T1 WHERE T1.TABLE_SCHEMA = '{0}' ORDER BY TABLE_NAME".format(self.targDBName)
            self.printLog(sql_getTargTabList)
            self.openTargetCursor()
            self.targCursor.execute(sql_getTargTabList)
            rows = self.targCursor.fetchall()
            targ_tab_list = []
            for row in rows:
                self.printLog(str(row[1]))
                targ_tab_list.append(row[1])

            for i in range(self.tabSour.rowCount()):
                tab_nm = self.tabSour.item(i,self.dic_tabResHeader['S_TAB_NM'] ).text()
                for targ_tab_nm in targ_tab_list:
                    if tab_nm.upper() == targ_tab_nm.upper():
                        self.tabSour.setItem(i, self.dic_tabResHeader['T_TAB_SCH'] ,QTableWidgetItem(self.targDBName))
                        self.tabSour.setItem(i, self.dic_tabResHeader['T_TAB_NM'] , QTableWidgetItem(targ_tab_nm))
                        break

            self.OnCheckBtn()
        except Exception as ex:
            self.printLog('Error:' + str(ex))
        finally:
            self.closeTargetCursor()


    def btnMakeDDLClicked(self):
        try:
            self.OffMigBtn()
            self.sourCursor = self.sourConn.cursor()
            QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
            if self.chkConnect() != 1:
                return

            # 선택된 테이블의 DDL 자동 만들기
            openFile = ".\mssql_to_mysql_make_ddl.sql"
            res = open(openFile, 'r', encoding='UTF8')
            sql_ddl = ""
            for line in res.readlines():
                sql_ddl = sql_ddl + line

            self.txtScript.setText('')
            for i in range(self.tabSour.rowCount()):
                print('loop',i)
                if self.tabSour.cellWidget(i, 0).isChecked():
                    self.prgBar.setValue(int(i / self.tabSour.rowCount() * 100))
                    #print('P', str(i) ,str(self.tabSour.rowCount()))

                    tab_nm = self.tabSour.item(i,self.dic_tabResHeader['S_TAB_NM']).text()
                    print(tab_nm, i)
                    exec_sql_ddl = sql_ddl.format(self.sourDBName, tab_nm ,self.sourDBName, tab_nm, self.targDBName)
                    self.sourCursor.execute(exec_sql_ddl)
                    rows = self.sourCursor.fetchall()
                    txt_ddl = '\n'
                    for row in rows:
                        txt_ddl = txt_ddl + row[0] + '\n'
                    self.txtScript.insertPlainText(txt_ddl)
                    self.txtScript.ensureCursorVisible()
                    QApplication.processEvents(QtCore.QEventLoop.ExcludeUserInputEvents)
                    #print(txt_ddl)

            self.printLog("DDL 생성이 완료되었습니다. 카피해서 별도 실행하세요.")
            self.prgBar.setValue(100)

        except Exception as ex:
            self.printLog('Error:' + str(ex))
        finally:
            QApplication.restoreOverrideCursor()
            self.sourCursor.close()
            self.OnMigBtn()
            self.OnCheckBtn()

    def make_mssql_get_data_all(self,_col_list, _tab_cat, _tab_sch, _tab_nm):
        sql = "SELECT  "
        i = 0
        for col in _col_list:
            if i == 0:
                sql = sql + " [" + col + "]"
                i = 1
            else:
                sql = sql + ",[" + col + "]"

        sql = sql + " FROM {0}.{1}.{2}".format(_tab_cat, _tab_sch, _tab_nm)
        self.printLog(sql)
        return sql

    def make_mysql_ins(self, _col_list, _t_tab_sch, _t_tab_nm):
        try:
            sql = "INSERT INTO {0}.{1} (".format(_t_tab_sch, _t_tab_nm)
            val = "VALUES ("
            i = 0
            for col in _col_list:
                if i == 0:
                    sql = sql + " `" + col + "`"
                    val = val + "%s"
                    i = 1
                else:
                    sql = sql + ",`" + col + "`"
                    val = val + ",%s"

            sql = sql + ")"
            val = val + ");"
            sql = sql + val
            return sql

        except Exception as ex:
            self.printLog('Error(make_mysql_ins):' + str(ex))
            return

    def btnMigStartClicked(self):
        # 이행 시작 처리
        if self.chkConnect() != 1:
            return

        msg = "Target DB(" + self.targDBName +")의 대상 테이블을 TRUNCATE 후 이행 처리합니다.\n"
        msg = msg + "복구가 불가능합니다. 진행하시겠습니까?"
        ret = QMessageBox.question(self, '확인!', msg, QMessageBox.Yes | QMessageBox.No)
        if ret == QMessageBox.No:
            return

        QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        QApplication.processEvents(QtCore.QEventLoop.ExcludeUserInputEvents)

        if ret == QMessageBox.Yes:
            try:

                self.sourCursor = self.sourConn.cursor()

                mig_tab_count = 0
                mig_tab_fail_count = 0
                mig_tab_succ_count = 0

                self.OffMigBtn()
                self.curRow = 0

                total_start_time = d.datetime.now()
                total_start_time_str = d.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                target_TableCount = 0

                for i in range(self.tabSour.rowCount()):
                    if self.tabSour.cellWidget(i, 0).isChecked():
                        target_TableCount = target_TableCount + 1

                if target_TableCount <= 0:
                    QApplication.restoreOverrideCursor()
                    QMessageBox.information(self, "Info","이행 대상이 없습니다.")
                    self.OnMigBtn()
                    self.OnCheckBtn()
                    return;

                if target_TableCount > 0:
                    for i in range(self.tabSour.rowCount()):
                        try:
                            self.curRow = i
                            self.prgBar.setValue(int(mig_tab_count / target_TableCount * 100))

                            if self.tabSour.cellWidget(i, 0).isChecked():
                                mig_tab_count = mig_tab_count + 1

                                it = self.tabSour.item(i, 1)
                                self.tabSour.scrollToItem(it)
                                self.tabSour.itemAt(i,1)
                                QApplication.processEvents(QtCore.QEventLoop.ExcludeUserInputEvents)

                                s_tab_cat = self.tabSour.item(i, self.dic_tabResHeader['S_TAB_CAT']).text()
                                s_tab_sch = self.tabSour.item(i, self.dic_tabResHeader['S_TAB_SCH']).text()
                                s_tab_nm = self.tabSour.item(i, self.dic_tabResHeader['S_TAB_NM']).text()
                                if self.tabSour.item(i, self.dic_tabResHeader['T_TAB_NM']) is None:
                                    t_tab_sch = ''
                                    t_tab_nm = ''
                                else:
                                    t_tab_sch = self.tabSour.item(i, self.dic_tabResHeader['T_TAB_SCH']).text()
                                    t_tab_nm = self.tabSour.item(i, self.dic_tabResHeader['T_TAB_NM']).text()

                                if t_tab_nm != '':
                                    table_start_time = d.datetime.now()
                                    table_start_time_str = d.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                                    mysql_trunc_table = "TRUNCATE TABLE {0}.{1};".format(t_tab_sch, t_tab_nm)
                                    mssql_get_data_top1 = "SELECT TOP 1 * FROM {0}.{1}.{2}".format(s_tab_cat,s_tab_sch,s_tab_nm)

                                    self.printLog(mssql_get_data_top1)
                                    self.sourCursor.execute(mssql_get_data_top1)

                                    # 컬럼명 처리
                                    col_list = []
                                    for desc in self.sourCursor.description:
                                        col_list.append(desc[0])

                                    exec_sql = self.sql_getDataCount_MSSQL.format(s_tab_cat, s_tab_sch, s_tab_nm)
                                    self.sourCursor.execute(exec_sql)
                                    source_data_count = self.sourCursor.fetchone()[0]
                                    self.tabSour.setItem(i, self.dic_tabResHeader['S_TAB_CNT'], QTableWidgetItem(str(source_data_count)))
                                    self.tabSour.setItem(i, self.dic_tabResHeader['T_TAB_CNT'],QTableWidgetItem(str(0)))

                                    if source_data_count <= 0:
                                        self.tabSour.setItem(i, self.dic_tabResHeader['MIG_RES'], QTableWidgetItem("이행완료"))
                                        mig_tab_succ_count = mig_tab_succ_count + 1

                                    if source_data_count > 0:
                                        self.openTargetCursor()
                                        self.targCursor.execute(mysql_trunc_table)
                                        self.closeTargetCursor()
                                        getAll_sql = self.make_mssql_get_data_all(col_list, s_tab_cat,s_tab_sch,s_tab_nm)
                                        # msCursor.execute(getAll_sql)
                                        # data_rows = msCursor.fetchall()
                                        isFin = 1
                                        self.sourCursor.execute(getAll_sql)
                                        read_count = 0
                                        self.openTargetCursor() # INSERT를 위해 커서 오픈
                                        while (isFin):
                                            insert_start_time = d.datetime.now()
                                            insert_start_time_str = d.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                            rows = self.sourCursor.fetchmany(size=self.get_fetch_size)
                                            df = pd.DataFrame(data=rows)
                                            if len(df) <= 0:
                                                isFin = 0  # EXIT
                                            if len(df) > 0:
                                                insSql_forMY = self.make_mysql_ins(col_list, t_tab_sch,t_tab_nm)
                                                read_count = read_count + len(df)
                                                # print(data_rows)
                                                insert_val_list = []
                                                for index, df_row in df.iterrows():
                                                    val_list = []
                                                    for col in df_row:
                                                        #print('TYPE:', type(col), str(type(col)))
                                                        #print(col)
                                                        if type(col) is bytes:
                                                            res = codecs.encode(col, 'hex_codec')
                                                            valStr = '0x' + res.decode('UTF-8').upper()
                                                        else:
                                                            valStr = str(col)

                                                        if valStr == 'None':
                                                            valStr = 'NULL'
                                                        elif valStr == 'NaT':
                                                            valStr = 'NULL'
                                                        elif valStr == 'nan':
                                                            valStr = 'NULL'

                                                        if valStr == 'NULL':
                                                            val_list.append(None)
                                                        else:
                                                            valStr = valStr.replace("'", "''")
                                                            try:
                                                                valStr = valStr.encode('ISO-8859-1').decode('euc-kr')
                                                            except UnicodeDecodeError:
                                                                valStr = valStr.encode('ISO-8859-1').decode('cp949')
                                                            except UnicodeEncodeError:
                                                                valStr = valStr
                                                            valStr = valStr.replace('\\', '\\\\')
                                                            valStr = valStr.strip()
                                                            val_list.append(valStr)

                                                    # one row end
                                                    insert_val_list.append(val_list)
                                                    # LOOP FINISH

                                                now_time = d.datetime.now()
                                                now_time_str = d.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                                                #self.openTargetCursor()
                                                self.targCursor.fast_executemany = True
                                                # self.targCursor.execute('SET SESSION bulk_insert_buffer_size = 1024 * 1024 * 256')  # 256Mega
                                                self.targCursor.executemany(insSql_forMY, insert_val_list)
                                                self.targCursor.execute('commit')
                                                insert_val_list = []
                                                #self.closeTargetCursor()

                                                strLog = "\n[ TOTAL PROGRESS: " + str(mig_tab_count) + "/" + str(target_TableCount) + ' - ' + str(round(mig_tab_count / target_TableCount * 100, 2)) + '%'
                                                strLog = strLog + "\n[ TOTAL PROGRESS TIME: " + total_start_time_str + "~" + now_time_str + "(" + str(round((now_time - total_start_time).total_seconds() / 60,1)) + " Min)"
                                                strLog = strLog + "\n  - " + t_tab_nm + " TABLE INSERT PROGRESS: " + str(read_count) + "/" + str(source_data_count) + ' - ' + str(round(read_count / source_data_count * 100, 2)) + '%'
                                                strLog = strLog + "\n  - " + t_tab_nm + " TABLE INSERT TIME: " + table_start_time_str + "~" + now_time_str + "(" + str(round((now_time - table_start_time).total_seconds() / 60,1)) + " Min)"
                                                strLog = strLog + "\n  - " + t_tab_nm + " FETCH " + str(len(df)) + " INSERT TIME: " + insert_start_time_str + "~" + now_time_str + "(" + str(round((now_time - insert_start_time).total_seconds(), 2)) + " Sec)"
                                                self.printLog(strLog)

                                        self.closeTargetCursor()

                                        # COUNT Target
                                        exec_sql = self.sql_getDataCount_MySQL.format(t_tab_sch, t_tab_nm)
                                        self.printLog(exec_sql)
                                        self.openTargetCursor()
                                        self.targCursor.execute(exec_sql)
                                        target_data_count = self.targCursor.fetchone()[0]
                                        self.tabSour.setItem(i, self.dic_tabResHeader['T_TAB_CNT'],QTableWidgetItem(str(target_data_count)))
                                        self.closeTargetCursor()
                                        # self.targConn.close()

                                        if target_data_count == source_data_count:
                                            self.tabSour.setItem(i, self.dic_tabResHeader['MIG_RES'],QTableWidgetItem("이행완료"))
                                            mig_tab_succ_count = mig_tab_succ_count + 1
                                        else:
                                            self.tabSour.setItem(i, self.dic_tabResHeader['MIG_RES'], QTableWidgetItem("이행실패"))
                                            mig_tab_fail_count = mig_tab_fail_count = 1


                        except Exception as ex:
                            self.printLog('Error:' + str(ex))
                            self.tabSour.setItem(self.curRow, self.dic_tabResHeader['MIG_RES'],QTableWidgetItem("실패:" + str(ex)))
                            mig_tab_fail_count = mig_tab_fail_count = 1
                            if self.isTargetCursorOn == 1:
                                self.closeTargetCursor()


                    # LOOP END;
                    self.prgBar.setValue(100)
                    #QMessageBox.information(self, "이행을 완료했습니다.",)
                    #QApplication.restoreOverrideCursor()
                    #QMessageBox.warning(self, "Error", "이행을 완료했습니다.")
                    QApplication.restoreOverrideCursor()
                    QMessageBox.information(self, "Info", "이행을 완료했습니다.\n"+'전체:'+str(mig_tab_count) + '\n'+ "성공:"+str(mig_tab_succ_count)+'\n'+'실패:'+str(mig_tab_fail_count))

            except Exception as ex:
                self.printLog('Error:' + str(ex))
                #self.tabSour.setItem(self.curRow, self.dic_tabResHeader['MIG_RES'], QTableWidgetItem("실패:"+str(ex)))
            finally:
                QApplication.restoreOverrideCursor()
                self.sourCursor.close()
                self.OnMigBtn()
                self.OnCheckBtn()

    def btnNoSourMapCheckClicked(self):
        for i in range(self.tabSour.rowCount()):
            if self.tabSour.item(i, self.dic_tabResHeader['T_TAB_NM']) is None:
                cbox = QCheckBox()
                cbox.setChecked(True)
                cbox.setStyleSheet("margin-left:50%; margin-right:50%;")
                self.tabSour.setCellWidget(i, 0, cbox)
            else:
                cbox = QCheckBox()
                cbox.setChecked(False)
                cbox.setStyleSheet("margin-left:50%; margin-right:50%;")
                self.tabSour.setCellWidget(i, 0, cbox)

    def btnSourMapCheckClicked(self):
        for i in range(self.tabSour.rowCount()):
            if self.tabSour.item(i, self.dic_tabResHeader['T_TAB_NM']) is None:
                cbox = QCheckBox()
                cbox.setChecked(False)
                cbox.setStyleSheet("margin-left:50%; margin-right:50%;")
                self.tabSour.setCellWidget(i, 0, cbox)
            else:
                cbox = QCheckBox()
                cbox.setChecked(True)
                cbox.setStyleSheet("margin-left:50%; margin-right:50%;")
                self.tabSour.setCellWidget(i, 0, cbox)

    def btnSourAllCheckClicked(self):
        for i in range(self.tabSour.rowCount()):
            cbox = QCheckBox()
            cbox.setChecked(True)
            cbox.setStyleSheet("margin-left:50%; margin-right:50%;")
            self.tabSour.setCellWidget(i, 0, cbox)


    def btnSourAllUnCheckClicked(self):

        for i in range(self.tabSour.rowCount()):
            cbox = QCheckBox()
            cbox.setChecked(False)
            cbox.setStyleSheet("margin-left:50%; margin-right:50%;")
            self.tabSour.setCellWidget(i, 0, cbox)


    def btnSourConnectClicked(self):
        self.OffMigBtn()
        self.sourConnState = 0
        self.sourDBType = 'MS SQL' #self.cboSourDBType.?
        self.sourHost = self.txtSourHost.text()
        self.sourPort = self.txtSourPort.text()
        self.sourUserID = self.txtSourUserID.text()
        self.sourPwd = self.txtSourPwd.text()
        self.sourDBName = self.txtSourDBName.text()
        self.loginSourceDB()
        print("CLICKED LOGIN", self.sourHost,self.sourPort,self.sourPwd)

    def btnTargConnectClicked(self):
        self.OffMigBtn()
        self.targConnState = 0
        self.targDBType = 'MySQL'
        self.targHost = self.txtTargHost.text()
        self.targPort = self.txtTargPort.text()
        self.targUserID = self.txtTargUserID.text()
        self.targPwd = self.txtTargPwd.text()
        self.targDBName = self.txtTargDBName.text()
        self.loginTargetDB()


    def getSourceTableList(self,_srch_word):
        if self.sourDBType == 'MS SQL':
            sql_getTableList = self.sql_getTableList_MSSQL.format(self.sourDBName,_srch_word)
            self.printLog(sql_getTableList)
            self.sourCursor = self.sourConn.cursor()
            self.sourCursor.execute(sql_getTableList)
            rows = self.sourCursor.fetchall()
            self.fillSourceTableList(rows)
            self.sourCursor.close()
            self.printLog("READ MS SQL COMPLETED")

    # 2022.01.27 added
    def getSourceTableListUseIn(self,_srch_word):
        if self.sourDBType == 'MS SQL':
            words = _srch_word.split(',')
            in_str = ""
            i=0
            for word in words:
                if i==0:
                    in_str = in_str+ "'" + word.replace("'","").replace(",","") + "'"
                    i=i+1;
                else:
                    in_str = in_str+ ",'" + word.replace("'", "").replace(",", "") + "'"

            sql_in = """SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES T1 WHERE T1.TABLE_CATALOG = '{0}' AND T1.TABLE_TYPE = 'BASE TABLE' AND T1.TABLE_NAME IN ({1})"""

            sql_getTableList = sql_in.format(self.sourDBName,in_str)
            self.printLog(sql_getTableList)
            self.sourCursor = self.sourConn.cursor()
            self.sourCursor.execute(sql_getTableList)
            rows = self.sourCursor.fetchall()
            self.fillSourceTableList(rows)
            self.sourCursor.close()
            self.printLog("READ MS SQL COMPLETED")


    def btnSaveTabClicked(self):

        try:
            l_S_TAB_CAT = []
            l_S_TAB_SCH = []
            l_S_TAB_NM = []
            l_S_TAB_CNT = []
            l_T_TAB_SCH = []
            l_T_TAB_NM = []
            l_T_TAB_CNT = []
            l_MIG_RES = []

            for i in range(self.tabSour.rowCount()):

                if self.tabSour.cellWidget(i, 0).isChecked():
                    s_tab_cat = self.tabSour.item(i, self.dic_tabResHeader['S_TAB_CAT']).text()
                    s_tab_sch = self.tabSour.item(i, self.dic_tabResHeader['S_TAB_SCH']).text()
                    s_tab_nm = self.tabSour.item(i, self.dic_tabResHeader['S_TAB_NM']).text()
                    if self.tabSour.item(i, self.dic_tabResHeader['S_TAB_CNT']) is None:
                        s_tab_cnt = ''
                    else:
                        s_tab_cnt = self.tabSour.item(i, self.dic_tabResHeader['S_TAB_CNT']).text()

                    if self.tabSour.item(i, self.dic_tabResHeader['T_TAB_NM']) is None:
                        t_tab_sch = ''
                        t_tab_nm = ''

                    else:
                        t_tab_sch = self.tabSour.item(i, self.dic_tabResHeader['T_TAB_SCH']).text()
                        t_tab_nm = self.tabSour.item(i, self.dic_tabResHeader['T_TAB_NM']).text()

                    if self.tabSour.item(i, self.dic_tabResHeader['T_TAB_CNT']) is None:
                        t_tab_cnt = ''
                    else:
                        t_tab_cnt = self.tabSour.item(i, self.dic_tabResHeader['T_TAB_CNT']).text()

                    if self.tabSour.item(i, self.dic_tabResHeader['MIG_RES']) is None:
                        mig_res = ''
                    else:
                        mig_res = self.tabSour.item(i, self.dic_tabResHeader['MIG_RES']).text()

                    l_S_TAB_CAT.append(s_tab_cat)
                    l_S_TAB_SCH.append(s_tab_sch)
                    l_S_TAB_NM.append(s_tab_nm)
                    l_S_TAB_CNT.append(s_tab_cnt)
                    l_T_TAB_SCH.append(t_tab_sch)
                    l_T_TAB_NM.append(t_tab_nm)
                    l_T_TAB_CNT.append(t_tab_cnt)
                    l_MIG_RES.append(mig_res)

            df = pd.DataFrame(list(zip(l_S_TAB_CAT,l_S_TAB_SCH,l_S_TAB_NM,l_S_TAB_CNT,l_T_TAB_SCH,l_T_TAB_NM,l_T_TAB_CNT,l_MIG_RES)),columns=['S-테이블카테','S-테이블스키','S-테이블명','S-건수','T-테이블스키','T-테이블명','T-건수','이행결과'])

            fname = os.getcwd() + "\\csv_res\\" + d.datetime.now().strftime("%Y%m%d_%H%M%S") + '이행결과.csv'
            df.to_csv(fname,encoding='utf-8-sig')
            QMessageBox.information(self, "Info", fname + " 에 저장했습니다.")



        except Exception as ex:
            self.printLog("Error",str(ex))



    def clearSourceTableList(self):
        self.tabSour.setColumnCount(0)
        self.tabSour.setRowCount(0)

    def fillSourceTableList(self, _rows):

        self.clearSourceTableList()

        self.tabSour.setColumnCount(len(self.dic_tabResHeader.keys()))
        self.tabSour.setRowCount(len(_rows))
        c=0
        for col_nm in self.dic_tabResHeader.keys():
            item = QTableWidgetItem(col_nm)
            self.tabSour.setHorizontalHeaderItem(c,item)
            c+=1

        r = 0
        for row in _rows:
            cbox = QCheckBox()
            cbox.setStyleSheet("margin-left:50%; margin-right:50%;")
            self.tabSour.setCellWidget(r, 0, cbox)
            c = 1

            item = QTableWidgetItem('')
            item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
            self.tabSour.setItem(r, self.dic_tabResHeader['S_TAB_CAT'], item)
            self.tabSour.setItem(r, self.dic_tabResHeader['S_TAB_SCH'], item)
            self.tabSour.setItem(r, self.dic_tabResHeader['S_TAB_NM'], item)
            self.tabSour.setItem(r, self.dic_tabResHeader['S_TAB_CNT'], item)
            self.tabSour.setItem(r, self.dic_tabResHeader['T_TAB_SCH'], item)
            self.tabSour.setItem(r, self.dic_tabResHeader['T_TAB_NM'], item)
            self.tabSour.setItem(r, self.dic_tabResHeader['T_TAB_CNT'], item)
            self.tabSour.setItem(r, self.dic_tabResHeader['MIG_RES'], item)

            for col in row:
                item = QTableWidgetItem(str(col))
                item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
                self.tabSour.setItem(r, c, item)
                #self.tabSour.setItem(r,c, QTableWidgetItem(col))
                c+=1
            r+=1

    def openTargetCursor(self):
        self.targConn = pymysql.connect(host=self.targHost, user=self.targUserID, password=self.targPwd,database=self.targDBName, charset='utf8', port=int(self.targPort))
        self.targCursor = self.targConn.cursor()
        self.isTargetCursorOn = 1

    def closeTargetCursor(self):
        self.isTargetCursorOn = 0
        self.targCursor.close()
        self.targConn.close()

    def loginTargetDB(self):
        try:
            if self.targDBType == 'MySQL':
                self.openTargetCursor()
                test_sql = "SELECT 1;"

                self.targCursor.execute(test_sql)
                rows = self.targCursor.fetchall()
                self.closeTargetCursor()
                if len(rows) <= 0:
                    self.printLog("Error: 타겟DB 접속이 불가능합니다.")
                else:
                    self.printLog("타겟 DB 접속 완료")
                    self.targConnState = 1
                    self.OnMigBtn()
                    # self.getSourceTableList()

        except Exception as ex:
            self.printLog("Error",str(ex))
        finally:
            self.clearSourceTableList()



    def loginSourceDB(self):
        try:
            if self.sourDBType == 'MS SQL':
                self.sourConn = pymssql.connect(host=self.sourHost, user=self.sourUserID, password=self.sourPwd, database= self.sourDBName,charset='utf8', port = self.sourPort)
                test_sql = "SELECT 1;"
                self.sourCursor = self.sourConn.cursor()
                self.sourCursor.execute(test_sql)
                rows = self.sourCursor.fetchall()
                self.sourCursor.close()
                if len(rows) <= 0:
                    print('Error')
                    self.printLog("Error: 소스DB 접속이 불가능합니다.")
                else:
                    self.printLog("소스 DB 접속 완료")
                    self.sourConnState = 1
                    #self.getSourceTableList()
                    self.OnMigBtn()
        except Exception as ex:
            self.printLog("Error",str(ex))
        finally:
            self.clearSourceTableList()


if __name__ == "__main__" :
    app = QApplication(sys.argv)
    window = migMini_MS_TO_MY()
    app.exec_()
