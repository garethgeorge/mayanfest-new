# Live Inspance IP
```
ssh 169.231.235.232
./mkfs.myfs /dev/vdb 32212254720 
./myfs /dev/vdb -f mountpoint 
```

# Useful Commands 

figure out how big a block level device is
```
sudo parted /dev/sda unit B p 
```
find available block level devices
```
fdisk -l
```
the size of our block level device is /dev/vdc = 107374182400

format and mount /dev/vdc
```
./mkfs.myfs /dev/vdc 107374182400
./myfs /dev/vdc -f mountpoint -o allow_other 
```
