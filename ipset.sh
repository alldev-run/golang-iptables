# IPv4 ipset and iptables
ipset create blacklist4 hash:ip family inet timeout 600
iptables -I INPUT -m set --match-set blacklist4 src -j DROP

# IPv6 ipset and ip6tables
ipset create blacklist6 hash:ip family inet6 timeout 600
ip6tables -I INPUT -m set --match-set blacklist6 src -j DROP
