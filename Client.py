import socket	
HOST = 'localhost'		#host
PORT = 9999			#port
sock =  socket.socket(socket.AF_INET,  socket.SOCK_STREAM) #socket initialization 
sock.bind((HOST,PORT))					   #bind host and port
sock.listen(5)						   #listen to socket
connect,addr = sock.accept()
while 1:						   #keep receiving
	buf = connect.recv(1024)
	if(len(buf)>0):
		print buf

