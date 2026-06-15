import sys
import os 
from opcua import Server
import time
import modbus_tk.modbus_tcp as mt
import modbus_tk.defines as md
import yaml

        

def main():    
    
    config_path = sys.argv[1]+"./config.yaml"
    
    if not os.path.exists(config_path):
        return
    with open(config_path,'r',encoding='utf-8') as f:
        cont = f.read()
        x = yaml.load(cont,Loader=yaml.FullLoader)
        modbus_ip = x.get('modbusslave').get('SlaveIp')
        modbus_port = x.get('modbusslave').get('SlavePort')
        supportNum = x.get('system').get('SupportNum')
        opcua_ip = x.get('opcuaserver').get('OpcuaIP')
        opcua_port = x.get('opcuaserver').get('OpcuaPort')
    #modbus客户端
    master = mt.TcpMaster(modbus_ip, int(modbus_port))
    master.set_timeout(5.0)
    #opcua服务端
    server = Server()
    server.set_endpoint("opc.tcp://"+opcua_ip+":"+str(opcua_port)+"/")
    uri = "www.lianli.com"
    idx = server.register_namespace(uri)
    objects = server.get_objects_node()
    root_obj = objects.add_object(idx, "LLSJSC")
    second_obj = []
    second_obj.append(root_obj.add_object(idx, "SHEAER"))
    for i in range(0,supportNum):
        if i<9:
            second_obj.append(root_obj.add_object(idx, "SUPPORT00"+str(i+1)))
        elif i>8 and i<99:
            second_obj.append(root_obj.add_object(idx, "SUPPORT0"+str(i+1)))
        else:
            second_obj.append(root_obj.add_object(idx, "SUPPORT"+str(i+1)))

    shearer_part = ["shearer_step","shearer_position","shearer_speed","shearer_direction"]
    shearer_part_object_list = []
    for n in shearer_part:
        shearer_part_object_list.append(second_obj[0].add_variable(idx,n,0))
        shearer_part_object_list[-1].set_writable()
    
    support_part_object_list =[]

    for i in range(0,supportNum):
        support_part_object_list.append([])
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"emergency_stop",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"lock",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"wifi_fault",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"can_fault",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"left_column_pressure",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"right_column_pressure",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"moving_distance",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"roof_height",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"top_plate_x_axis_inclination",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"top_plate_y_axis_inclination",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"base_x_axis_inclination",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"base_y_axis_inclination",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"reserve1",0))
        support_part_object_list[i].append(second_obj[i+1].add_variable(idx,"reserve2",0))
    server.start()
    try:
        while True:
            try:
                shearer_temp = master.execute(slave=1, function_code=md.READ_HOLDING_REGISTERS, starting_address=179, quantity_of_x=4)
                shearer_part_object_list[0].set_value(shearer_temp[0])
                shearer_part_object_list[1].set_value(shearer_temp[1])
                shearer_part_object_list[2].set_value(shearer_temp[2])
                shearer_part_object_list[3].set_value(shearer_temp[3])
                for i in range(0,supportNum):
                    #故障闭锁急停数据
                    support_fault = master.execute(slave=1, function_code=md.READ_HOLDING_REGISTERS, starting_address=4440+i, quantity_of_x=1)
                    #故障闭锁急停数据是否可信
                    if len(bin(support_fault[0]))<11:
                        add_str = ""
                        for k in range(0,6-len(bin(support_fault[0]))):
                            add_str +="0"
                        support_information = "0b"+add_str+bin(support_fault[0])[2:]
                        support_part_object_list[i][0].set_value(int(support_information[-4]))
                        support_part_object_list[i][1].set_value(int(support_information[-3]))
                        support_part_object_list[i][2].set_value(int(support_information[-2]))
                        support_part_object_list[i][3].set_value(int(support_information[-1]))
                    else:
                        support_part_object_list[i][0].set_value(-1)
                        support_part_object_list[i][1].set_value(-1)
                        support_part_object_list[i][2].set_value(-1)
                        support_part_object_list[i][3].set_value(-1)
                    #模拟量数据
                    support_simulation = master.execute(slave=1, function_code=md.READ_HOLDING_REGISTERS, starting_address=1520+i*9, quantity_of_x=9)
                    if i==10:
                        print(support_simulation)
                    support_part_object_list[i][4].set_value(support_simulation[0])
                    support_part_object_list[i][5].set_value(support_simulation[1])
                    support_part_object_list[i][6].set_value(support_simulation[2])
                    support_part_object_list[i][7].set_value(support_simulation[3])
                    support_part_object_list[i][8].set_value(support_simulation[4])
                    support_part_object_list[i][9].set_value(support_simulation[5])
                    support_part_object_list[i][10].set_value(support_simulation[6])
                    support_part_object_list[i][11].set_value(support_simulation[7])
                    support_part_object_list[i][12].set_value(support_simulation[8])
                time.sleep(1)
            except Exception as e:
                print(e)
                master = mt.TcpMaster(modbus_ip, int(modbus_port))
                master.set_timeout(5.0)
    finally:
        server.stop()

if __name__ == "__main__":
    main()