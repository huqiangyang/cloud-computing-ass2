#
#Team:15
#Yuqi Cao 1186642
#Yaxuan Huang 1118801
#Meng Yang 1193990
#Gangdan Shu 1239032
#Zheng Xu 1291694
#
nodes=()
i=0
ip_addr=''
for arg in $*
do
  if [ "$arg" == "--ip-addr" ]; then
    break
  fi
  ip=`echo $arg | grep -o '[0-9]\{1,3\}[.][0-9]\{1,3\}[.][0-9]\{1,3\}[.][0-9]\{1,3\}'`
  nodes[i]=$ip
  ((i++))
done
r_nodes=${nodes[*]}
nodes[i]=172.26.255.255
((i++))
((i++))
ip_addr=$(eval echo '$'${i})
export masternode=`echo ${nodes} | cut -f1 -d' '`
declare -a othernodes=`echo ${nodes[@]} | sed s/${masternode}//`
export user='yuqicao'
export pass='7486ocean'
export VERSION='3.2.1'
export cookie='a192aeb9904e6590849337933b000c99'
# ip_addr=`ifconfig -a|grep -A 2 eth0|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'`
docker pull ibmcom/couchdb3:${VERSION}


for node in "${nodes[@]}"
  do
    if [ ! -z $(docker ps --all --filter "name=couchdb${node}" --quiet) ]
       then
         docker stop $(docker ps --all --filter "name=couchdb${node}" --quiet)
         docker rm $(docker ps --all --filter "name=couchdb${node}" --quiet)
    fi
done

container=`docker create\
            -p 5984:5984 \
            -p 4369:4369 \
            -p 9100-9200:9100-9200 \
            --name couchdb${ip_addr}\
            --env COUCHDB_USER=${user}\
            --env COUCHDB_PASSWORD=${pass}\
            --env COUCHDB_SECRET=${cookie}\
            --env ERL_FLAGS="-setcookie \"${cookie}\" -name \"couchdb@${ip_addr}\""\
            ibmcom/couchdb3:${VERSION}`

docker start ${container}


if [ "$ip_addr" != "$masternode" ]; then
  exit 0
fi

sleep 6
for node in ${othernodes}
do
	if [ "$node" = "172.26.255.255" ]; then
		continue
	fi
    curl -XPOST "http://${user}:${pass}@${masternode}:5984/_cluster_setup" \
      --header "Content-Type: application/json"\
      --data "{\"action\": \"enable_cluster\", \"bind_address\":\"0.0.0.0\",\
             \"username\": \"${user}\", \"password\":\"${pass}\", \"port\": \"5984\",\
             \"remote_node\": \"${node}\", \"node_count\": \"${#r_nodes[@]}\",\
             \"remote_current_user\":\"${user}\", \"remote_current_password\":\"${pass}\"}"
done

for node in ${othernodes}
do
  	if [ "$node" = "172.26.255.255" ]; then
		  continue
	  fi
    curl -XPOST "http://${user}:${pass}@${masternode}:5984/_cluster_setup"\
      --header "Content-Type: application/json"\
      --data "{\"action\": \"add_node\", \"host\":\"${node}\",\
             \"port\": \"5984\", \"username\": \"${user}\", \"password\":\"${pass}\"}"
done

curl -XPOST "http://${user}:${pass}@${masternode}:5984/_cluster_setup"\
    --header "Content-Type: application/json" --data "{\"action\": \"finish_cluster\"}"


for node in ${r_nodes}; do curl -X GET "http://${user}:${pass}@${node}:5984/_membership"; done


curl -XPUT "http://${user}:${pass}@${masternode}:5984/twitter"

for node in ${r_nodes}; do curl -X GET "http://${user}:${pass}@${node}:5984/_all_dbs"; done


