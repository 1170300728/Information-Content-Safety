import pcap
import dpkt
from pylibpcap import get_iface_list
from pylibpcap import get_first_iface



print(get_iface_list())

print(get_first_iface())

sniffer = pcap.pcap(name=get_first_iface())  
sniffer.setfilter("tcp")                 
i=0  
for packet_time, packet_data in sniffer:
    print("***********")
    packet = dpkt.ethernet.Ethernet(packet_data)

    print("源IP:%d.%d.%d.%d" % tuple(list(packet.data.src)))

    print("目的IP:%d.%d.%d.%d" % tuple(list(packet.data.dst)))

    print("源端口:%d" % packet.data.data.sport)

    print("目的端口:%d" % packet.data.data.dport)

    t = packet.data.data.data[:]
    print("有效载荷:")
    print(t)

    print()
    if(i>=10):
        break
    else:
        i = i + 1
