package io.aiven.kafka.connect.gcs.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeadersWriter implements OutputFieldWriter {

    private final List<String> headers;

    HeadersWriter(List<String> headers){
        this.headers = headers;
    }

    private Map<String, String> getRecordHeaders(SinkRecord record) {
        Map<String, String> headers = new HashMap<>();
        if (record.headers() != null) {
            int headerCount = 0;
            for (Header header : record.headers()) {
                String key = header.key();
                if (this.headers.contains(key) && key.getBytes().length < 257 && String.valueOf(header.value()).getBytes().length < 1025) {
                    headers.put(key, String.valueOf(header.value()));
                    headerCount++;
                }
                if (headerCount > 100) {
                    break;
                }
            }
        }
        return headers;
    }


    @Override
    public void write(SinkRecord record, OutputStream outputStream) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        outputStream.write(mapper.writeValueAsBytes(getRecordHeaders(record)));
    }
}

