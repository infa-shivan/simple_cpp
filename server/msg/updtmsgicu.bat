@echo off
REM
REM Usage:
REM updtmsg <msgdir> <file> <headerdir>
REM
REM Where <file> is "rep", "cmn", "mq", etc. (no .h, _msg, etc)
REM and <msgdir> is the directory where the .msg file is in. (en, ja, etc)
REM and <headerdir> is the directory where the header file goes.
REM
REM Example:
REM  updtmsg.bat en rep ..\cmnmisc
REM

rem Generate the new stage file.

REM echo gcgencat -lang C -new junk.cat generated\%1\%2_msg.msg %2_msg.h
REM gcgencat -lang C -new junk.cat generated\%1\%2_msg.msg %2_msg.h
if not exist %3\%2_msg.h goto update
echo Comparing generated\%1\%2_msg.h %3\%2_msg.h
cmp.exe generated\%1\%2_msg.h %3\%2_msg.h

if errorlevel 1 goto update
goto uptodate

:update
echo Copying generated\%1\%2_msg.h to %3\%2_msg.h...
copy generated\%1\%2_msg.h %3\%2_msg.h

:uptodate
