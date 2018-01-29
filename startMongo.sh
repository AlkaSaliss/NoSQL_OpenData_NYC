echo "shell script called"
if pgrep mongod > /dev/null
then
    echo "MongoDB Running"
else
    echo "Stopped .. starting MongoDB"
        sudo service mongod start
fi
