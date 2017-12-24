package kafka;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import probuff.EmployeeOuterClass.Employee;

public class SerializerEmployee implements Serializer<Employee> {
	private String encoding = "UTF8";

	public void close() {

	}

	public void configure(Map<String, ?> arg0, boolean data) {
	}

	public byte[] serialize(String arg0, Employee data) {

		int sizeOfName;
		int sizeOfId;
		int sizeOfOffice;
		byte[] serializedName;
		byte[] serializedId;
		byte[] serializedOffice;

		try {
			if (data == null)
				return null;
	
			serializedName = data.getName().getBytes(encoding);
			sizeOfName = serializedName.length;
			
			
			serializedId = Integer.toString(data.getId()).getBytes(encoding);
			sizeOfId = serializedId.length;
			
			serializedOffice = data.getOffice().getBytes(encoding);
			sizeOfOffice = serializedOffice.length;

			ByteBuffer buf = ByteBuffer.allocate(4+sizeOfName+4+sizeOfId+4+sizeOfOffice);
			
			buf.putInt(sizeOfName);
			buf.put(serializedName);
			
			buf.putInt(sizeOfId);
			buf.put(serializedId);
			
			buf.putInt(sizeOfOffice);
			buf.put(serializedOffice);
			
			return buf.array();
			
			

		} catch (Exception e) {
			System.out.println(e.getStackTrace());
			throw new SerializationException("Error occured while serializing");
			
		}

		
	}


}