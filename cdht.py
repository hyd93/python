'''  xterm -hold -title "peer 1" -e "python3 cdht.py
 1 3 4 400 0.1" & xterm -hold -title "peer 4" -e "python3 cdht.py
 4 5 8 400 0.1"
 '''
#import multiprocessing as mp
import threading as td
import sys 
import socket
import time
import math
import random
#import pickle


global hashvalue
global requestfile_peer
global filenumber
original_requestfile_peer = 0
write_pdf = 0
global mss
depart = 0
predecessor = []
successor1_kill = 0
successor2_kill = 0
kill_1_change = 0

class mess_udp_file():
    def __init__(self, message):
        self.receiver = message
#        self.first = message[0]

class Peer(td.Thread):

    def __init__(self,port,successor_port,MSS,drop_rate):
        global mss
        td.Thread.__init__(self)
        mss = MSS
        self.id = port
        self.drop_rate = drop_rate
        self.port = port + 50000
        self.successor = successor_port
#        self.udp_recv = None
        
    def check_input(self):
        global hashvalue
        global filenumber
        global original_requestfile_peer
        global depart
        while 1:
            filename = input('')
            fn = filename.split(' ')
            if fn[0] == 'request' and len(fn[1]) == 4 and fn[1].isdigit():
                filenumber = int(fn[1])
                self.hash_function()
                print(hashvalue)
                print(f'File request message for {filenumber} has been sent to my successor.')
                original_requestfile_peer = 1
    #            while find_file:
                self.TCPClient(self.successor[0])   
                
#                TCP_client1 = td.Thread(target = self.TCPClient,args = (self.successor[0],))
#
#                TCP_client1.start()
            elif fn[0] == 'quit':
                depart = 1
            else:
                continue
    #            TCP_client1.join()

#find which peer to store file            
    def hash_function(self):
        global filenumber
        global hashvalue
        hashvalue = filenumber % 256
        
        
    def PingServer(self):
        global write_pdf
        flag_ack = 0
        global depart
        global predecessor
        
        serverName = '127.0.0.1'
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#        serverSocket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
        serverSocket.bind((serverName,self.port))

        
        while 1:
            data, addr = serverSocket.recvfrom(1024)
        #    clientSocket.settimeout(1)
            
#            print(type(data))
#            
#            sentence = pickle.loads(data)
#            print(sentence.receiver)
#            sen = sentence.receiver
            sen = data.decode(encoding='utf-8',errors='ignore')

            if sen[0] == 'P':
                sen = sen.split(' ')
                mess = str(self.id)
                serverSocket.sendto(mess.encode(),addr)
                print(f'A ping request message was received from Peer {int(sen[1])}.')
                if (len(predecessor) < 2) and (int(sen[1]) not in predecessor):
                    predecessor.append(int(sen[1]))
                if (len(predecessor) == 2) and (int(sen[1]) not in predecessor):
                    predecessor = []
#check if receive files
            elif sen[0] == 'f':
                head = sen[0:40]

                file_content = sen[40:]
                head= head.split(' ')
#                print(head)
                start_time = float(head[4])
                
                rcv_time = time.time() - start_time
                seq = int(head[1])
                ack = int(head[2])
                if ack != flag_ack:
                    log_request = open('requesting_log.txt','a+')
                    
                    
                    rcv = 'rcv '+ str(rcv_time) + ' ' + str(seq) + ' ' + str(len(sen)) + ' ' + str(ack) 
                    log_request.write(rcv + '\n')
                    
                    with open('receive_file.pdf','a+') as f_pdf:
                        f_pdf.write(file_content)
                        
                        f_pdf.close()
                    
                    responseACK = 'ACK ' + str(ack)  
                    serverSocket.sendto(responseACK.encode(),addr)
                    snd_time = time.time() - start_time
                    snd = 'snd ' + str(snd_time) + ' ' + str(seq) + ' ' + str(len(sen)) + ' ' + str(ack)
                    log_request.write(snd + '\n')
                    log_request.close()
#                    snd_time = time.time()
#                    snd = 'snd ' + str(snd_time) + ' ' + str(seq) + ' ' + str(len(sen[1])) + ' ' + str(ack)
#                    log_request.write(snd)
#                    log_request.close()
                    flag_ack = ack
#                    with open('receive_file.pdf','wb') as f_pdf:
#                        f_pdf.write(data)
#                        
#                        f_pdf.close()
#                    print(data)
            if depart == 1:
                self.TCP_depart(predecessor[0])
                self.TCP_depart(predecessor[1])
  
#            print(sentence)
#            if sen[0] == 'Ping':
#                mess = str(self.id)
#                serverSocket.sendto(mess.encode(),addr)
#                print(f'A ping request message was received from Peer {sen[1]}.')
#            else:
#                if write_pdf:
#                    
#            time.sleep(5)
                
    def PingClient(self):
        global mss
        global depart
        global successor1_kill
        global successor2_kill
        serverName = '127.0.0.1'
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#        clientSocket.bind((serverName,self.port))
        while 1:
#send ping to first successor
            try:
                clientSocket.settimeout(10)
                
                mess = 'Ping '+str(self.id)
                mess = mess.encode()
#                mess = mess_udp_file(['Ping ',str(self.id)])
#                print(mess)
                
                
              
                clientSocket.sendto(mess,(serverName, 50000 + self.successor[0]))
            #    print(mess)
                data, addr = clientSocket.recvfrom(mss)
                sentence = data.decode()
    #            sen = list(sentence.split())
    #            print(sentence)
                if sentence:
                    print(f'A ping response message was received from Peer {sentence}.')
                    successor1_kill = 0
            except socket.timeout:
                successor1_kill += 1
#                print(f'no response from {self.successor[0]}')
#send ping to second successor
            try:
                clientSocket.settimeout(15)
                mess = 'Ping ' + str(self.id)
                clientSocket.sendto(mess.encode(),(serverName, 50000 + self.successor[1]))
            #    print(mess)
                data, addr = clientSocket.recvfrom(mss)
                sentence = data.decode()
    #            sen = list(sentence.split())
    #            print(sentence)
                if sentence:
                    print(f'A ping response message was received from Peer {sentence}.')
                    successor2_kill = 0
            except socket.timeout:
                successor2_kill += 1
#                print(f'no response from {self.successor[1]}')
            time.sleep(5)
            if depart > 2:
                break
#for the nearest predecessor to check its first successor kill
            if successor1_kill == 3:
                print(f'Peer {self.successor[0]} is no longer alive.')
                self.TCP_kill_1()
#for the another predecessor to check its second successor kill
            if  successor2_kill == 3:
                print(f'Peer {self.successor[1]} is no longer alive.')
                self.TCP_kill_2()
     
    def TCPServer(self):
        global requestfile_peer
        global hashvalue
        global filenumber
        global mss
        global write_pdf
        global depart
        global predecessor
        global successor2_kill
        global kill_1_change
        serverName = '127.0.0.1'
         
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serverSocket.bind((serverName, self.port))
        serverSocket.listen(5)
        while 1:
            connectionSocket, addr = serverSocket.accept()
            data = connectionSocket.recv(mss)

            sentence = data.decode()
            sentence = sentence.split(' ')
#            print(sentence)
#get the message that its predecessor change and send its successor id
            if sentence[0] == 'kill1':
                mess = 'newsuccessor ' + str(self.successor[0])
                connectionSocket.send(mess.encode())
                predecessor = []
#get the message that its first predecessor is updated successors and send successor id to another predecessor               
            if sentence[0] == 'kill2' and kill_1_change == 1:
                mess = 'newsuccessor ' + str(self.successor[0])
                connectionSocket.send(mess.encode())
                predecessor = []
                kill_1_change = 0

#check if peer quit, change successor    
            if sentence[0] == 'quit':
                predecessor = []
                depart = 2
                if self.successor[0] == int(sentence[1]):
                    self.successor[0] = int(sentence[2])
                    self.successor[1] = int(sentence[3])
                elif self.successor[1] == int(sentence[1]):
                    self.successor[1] = int(sentence[2])
                mess = 'predcessor' + str(self.id)
                connectionSocket.send(mess.encode())
                print(f'Peer {sentence[1]} will depart from the network.')
                print(f'My first successor is now peer {self.successor[0]}.')
                print(f'My second successor is now peer {self.successor[1]}.')
# get message of file transmit,and then open file  write in udp receiver                
            if sentence[0] == 'respondingpeer':
                mess = 'Get respondingpeer'
                print(f'Received a response message from peer {int(sentence[1])}, which has the file {filenumber}.')
                write_pdf = 1
                
                connectionSocket.send(mess.encode())
#check original peer and hashvalue and predecessor
            if sentence[0].isdigit() and sentence[3].isdigit() and sentence[-1].isdigit():
                requestfile_peer = int(sentence[0])
                hashvalue = int(sentence[3])
                filenumber = int(sentence[4])
                mss = int(sentence[6])
                if hashvalue == self.id:
                    self.tcp_respondingpeer()
                elif int(sentence[-1]) < hashvalue < self.id or requestfile_peer == self.id:
                    self.tcp_respondingpeer()
#check if hashvalue biggger than all peer id
                elif self.id < int(sentence[-1]) < hashvalue:
                    self.tcp_respondingpeer()
                    
                else:
#TCP open thread or not are same             
                    print(f'File {filenumber} is not stored here.')
                    print('File request message has been forwarded to my successor.')
                    self.TCPClient(self.successor[0])
                    
#                    requestfile = td.Thread(target = self.TCPClient,args = (self.successor[0],))
#                    requestfile.start()
#                    requestfile.join()
#            time.sleep(10)
    def TCPClient(self,successor):
        global hashvalue
        global requestfile_peer
        global original_requestfile_peer
        global filenumber
        global mss
        if original_requestfile_peer == 1:
            requestfile_peer = self.id
            original_requestfile_peer = 0
        mess = str(requestfile_peer) + ' request file ' + str(hashvalue)+ ' ' + str(filenumber) + ' mss ' + str(mss) + ' send by ' + str(self.id)
        print(mess)
        serverName = '127.0.0.1'
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((serverName,50000 + successor))
        
#        while 1:
#            try:
#                clientSocket.settimeout(3)
                
        clientSocket.send(mess.encode())
#            data = clientSocket.recv(1024)
#            print(data.decode())
#        time.sleep(10)        
                
    def tcp_respondingpeer(self):
        global filenumber
        global requestfile_peer
        global mss

        print(f'File {filenumber} is here.\nA response message, destined for peer {requestfile_peer}, has been sent.')            
        
        serverName = '127.0.0.1'
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((serverName,50000 + requestfile_peer))
#        while 1:
        mess = 'respondingpeer ' + str(self.id)
        clientSocket.send(mess.encode())
        time.sleep(2)
        data = clientSocket.recv(mss)
        if data:
            print('We now start sending the file .........\nThe file is sent.')
#                filename = str(filenumber) + '.pdf'
#                f = open(filename,'rb')
#                content = f.read()
#                f.close()
#                print(content)
#                break
#            self.udp_source(50000 + requestfile_peer)
            udpsource = td.Thread(target = self.udp_source,args = (50000 + requestfile_peer,) )

            udpsource.start()
#            except socket.timeout:
#                print(f'request peer {successor} time out')
    def udp_source(self,port_receivefile):
        
        global filenumber
        global mss

        flag_rtx = 0
        serverName = '127.0.0.1'
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        filename = str(filenumber) + '.pdf'
        with open(filename,'rb') as f:
            content = f.read()
            f.close()
        seq = 1
        ack = 1
#get the number of packets
        x = math.ceil(len(content) / mss)
        start_time = time.time()
        log_response = open('responding_log.tx.', 'a+')
        
        while x:
            
            random_probability = random.uniform(0,1)
            
            
            try:
                clientSocket.settimeout(1)
#to check whether send the next packet, updating content need to transmit
                if flag_rtx == 0:
                    if len(content) > mss:
                        transmit = content[0:mss]
                        content = content[mss:]
                    else:
                        transmit = content
                header = 'file ' + str(seq) + ' ' + str(ack) + ' ' + str(len(transmit)) + ' ' + str(start_time)
                n = 40 - len(header)
                h = header + n * ' '
                mess = h.encode() + transmit

                
                if random_probability > self.drop_rate:
                    clientSocket.sendto(mess,(serverName, port_receivefile))
            #    print(mess)
                    snd_time = time.time() - start_time
                    if flag_rtx == 1:
                        snd = 'RTX ' + str(snd_time) + ' ' + str(seq) + ' ' + str(len(mess)) + ' ' + str(ack)
                        
                        flag_rtx = 0 
                    else:
                        snd = 'snd ' + str(snd_time) + ' ' + str(seq) + ' ' + str(len(mess)) + ' ' + str(ack)
                    log_response.write(snd + '\n')
                    data, addr = clientSocket.recvfrom(1024)
                    sentence = data.decode()
                    sentence = sentence.split(' ')
                    
#only get the ack, the seq renew
                    if sentence[0] == 'ACK' and int(sentence[1]) == ack:
                        rcv_time = time.time() - start_time
                        rcv = 'rcv ' + str(rcv_time) + ' ' + str(seq) + ' ' + str(len(mess)) + ' ' + str(ack)
                        log_response.write(rcv + '\n')
                        seq += 1
                        ack += 1
                        x -= 1
                else:#drop happen
                    drop_time = time.time() - start_time
                    drop = 'drop ' + str(drop_time) + ' ' + str(seq) + ' ' + str(len(mess)) + ' ' + str(ack)
                    log_response.write(drop + '\n')
                    flag_rtx = 1
            except socket.timeout:
                rtx_time = time.time() - start_time
                rtx = 'RTX ' + str(rtx_time) + ' ' + str(seq) + ' ' + str(len(mess)) + ' ' + str(ack)
                log_response.write(rtx + '\n')
                flag_rtx = 1
        log_response.close()
      
        
#    def udp_destination(self):
#        serverName = '127.0.0.1'
#        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#        serverSocket.bind((serverName,self.port))
#        while 1:
#            data, addr = serverSocket.recvfrom(1024)
#            sentence = data.decode()
    
    def TCP_depart(self,predecessor):
        global depart
    
        serverName = '127.0.0.1'
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((serverName,50000 + predecessor))
        mess = 'quit ' + str(self.id)  + ' ' + str(self.successor[0]) + ' ' + str(self.successor[1])
        clientSocket.send(mess.encode())
        data = clientSocket.recv(1024)
        if data:
            depart += 1
#the killed peer as successor 1 before, manage the first predecessor
    def TCP_kill_1(self):
        global kill_1_change
        serverName = '127.0.0.1'
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((serverName,50000 + self.successor[1]))
        mess = 'kill1 ' + str(self.successor[0])
        clientSocket.send(mess.encode())

        time.sleep(2)
        data = clientSocket.recv(1024)
        s = data.decode()
        s = s.split(' ')
        if data:
           
            self.successor[0] = self.successor[1]
            self.successor[1] = int(s[1])
            kill_1_change = 1  #to make sure the first predecessor is update successors
            print(f'My first successor is now peer {self.successor[0]}.')
            print(f'My second successor is now peer {self.successor[1]}.')
#the killed peer as successor 2 before,manage the second predecessor            
    def TCP_kill_2(self):
        serverName = '127.0.0.1'
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((serverName,50000 + self.successor[0]))
        mess = 'kill2 ' + str(self.successor[1])
        clientSocket.send(mess.encode())

        time.sleep(5)
        data = clientSocket.recv(1024)
        
        s = data.decode()
        s = s.split(' ')

        if data:
            self.successor[1] = int(s[1])
            print(f'My first successor is now peer {self.successor[0]}.')
            print(f'My second successor is now peer {self.successor[1]}.')
        
        
        
        
        
        
if __name__ ==  '__main__':
    cmd = sys.argv[1:]
#    print(cmd)
    
      
    
    if len(cmd) == 5:
        port = int(cmd[0])
        successor__port = [int(cmd[1]),int(cmd[2])]
        p = Peer(port,successor__port,int(cmd[3]),float(cmd[4]))
        
        keyboard_check = td.Thread(target = p.check_input,args = ())
        
        udpServer = td.Thread(target = p.PingServer,args = ())
        udpClient1 = td.Thread(target = p.PingClient,args = (),)
#        udpClient2 = td.Thread(target = p.PingClient,args = (),)
        
        TCP_server = td.Thread(target = p.TCPServer,args = ())
        keyboard_check.start()
        TCP_server.start()
        udpServer.start()
        udpClient1.start()
#        udpClient2.start()
        time.sleep(20)
        sys.exit()
#        keyboard_check.join()
#        keyboard_check.join()
#        TCP_server.join()
#        udpServer.join()
#        udpClient1.join()
#        udpClient2.join()
#        p.PingServer()
#        p.PingClient(int(cmd[1]),p.port)
#        p.PingClient(int(cmd[2]),p.port)
            
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
