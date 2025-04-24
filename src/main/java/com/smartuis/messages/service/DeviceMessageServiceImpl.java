package com.smartuis.messages.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.smartuis.messages.documents.DeviceMessage;
import com.smartuis.messages.dto.DeviceMessageDetail;
import com.smartuis.messages.repository.DeviceMessageRepository;

@Service
public class DeviceMessageServiceImpl  implements DeviceMessageService{

    @Autowired
    private DeviceMessageRepository repository;

    @Override
    public List<DeviceMessageDetail> findByDeviceUUID(String uuid){
        List<DeviceMessageDetail> list_messages = new ArrayList<>();
        //System.out.println(uuid);

        Iterable<DeviceMessage> db_list = repository.findBydeviceUUID(uuid);
        //Iterable<DeviceMessage> db_list = repository.findAll();

        db_list.forEach(message->{
            DeviceMessageDetail details = new DeviceMessageDetail();
            details.setEntity(message);
            list_messages.add(details);
        });

        return list_messages;
    }


}
