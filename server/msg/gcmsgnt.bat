	@echo off
REM (C) Informatica Corporation 2000
REM Generate the catalog file from the message file
REM
 
:Begin
if "%1"=="" goto Usage
if "%2"=="" goto Usage	
setlocal
set locale=%1


set TOOLS_HOME=..\..\..\..\powrmart\tools
set MSG_DIR=..\msg

set JAVA_CMD=%JAVA_HOME%\bin\java
set JAVA_EXE=%JAVA_CMD% -cp %TOOLS_HOME%\Message\Message.jar com.informatica.tools.message.Compiler

set OUTDIR=%MSG_DIR%\generated\%1
mkdir %OUTDIR%
del /Q /F %OUTDIR%

%JAVA_EXE% icu -xml=%MSG_DIR%\%1\*.xml -out=%OUTDIR% -xsd=%TOOLS_HOME%\Message\message.xsd -icuenc=%icuencoding% -icuhfile=%icuhfileformat%
if NOT %errorlevel% == 0 goto ERROR

set sMsgfiles=%OUTDIR%\rdr_msg.txt %OUTDIR%\wrt_msg.txt  %OUTDIR%\cmn_msg.txt

REM echo Generating PowerConnect for netezza message catalog file [%2]...
REM echo gcgencat %2 %sMsgfiles% 

call genicurb %1 %2 %sMsgfiles% 
rm %2_%1_icu_msgs.msg
endlocal
echo done
goto End
 
:Usage
echo %0 -- build the message catalog files
echo Error: %0 expecting two parameters!
echo Usage: 
echo   %0 {lang} {Catalog File Name}
echo Examples:
echo   %0 ja ..\Debug\pmnetzza_ja.res
echo   %0 en ..\Release\pmnetezza_en.res
echo   %0 pt_BR ..\Release\pmnetezza_PT_BR.res
goto End
 
:End

