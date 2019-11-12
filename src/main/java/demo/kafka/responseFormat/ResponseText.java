package demo.kafka.responseFormat;

public class ResponseText {
    private String response;
    private long offset;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
    public ResponseText(String response,long offset) {
        this.response = response;
        this.offset = offset;
    }
    public ResponseText() {

    }
    public ResponseText(String response) {
        this.response = response;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }
}
