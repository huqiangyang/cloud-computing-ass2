#
#Team:15
#Yuqi Cao 1186642
#Yaxuan Huang 1118801
#Meng Yang 1193990
#Gangdan Shu 1239032
#Zheng Xu 1291694
#
#!/bin/bash
ips=()
i=0
for arg in $*
do
        if [ "$arg" == "--ip-addr" ]; then
                break
        fi
        echo $arg
        ip=`echo $arg | grep -o '[0-9]\{1,3\}[.][0-9]\{1,3\}[.][0-9]\{1,3\}[.][0-9]\{1,3\}'`
        echo $ip
        ips[i]=$ip
        ((i++))
done
echo ${ips[*]}
r_ips=${ips[*]}
echo ${r_ips[*]}
ips[i]=172.26.255.255
echo ${ips[*]}
echo ${r_ips[*]}
((i++))
((i++))
echo $i
ip_addr=$(eval echo '$'${i})
echo $ip_addr
exit 1
