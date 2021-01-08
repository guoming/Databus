set workDir=%cd%
cd %workDir%/src
dotnet publish --runtime linux-x64  --framework netcoreapp3.1 --self-contained -c  Release -o  %workDir%/dist/linux-x64/canal-top-exchange
pause