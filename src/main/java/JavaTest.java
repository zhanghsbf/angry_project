import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JavaTest {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String x = "我爱你";
        byte[] bytes = x.getBytes(StandardCharsets.UTF_8);
        System.out.println(Arrays.toString(bytes));

        StringBuffer unicode = new StringBuffer();

        for (int i = 0; i < x.length(); i++) {

            // 取出每一个字符
            char c = x.charAt(i);

            // 转换为unicode
            unicode.append(String.format("\\u%04x", (int) c));
        }

        System.out.println(unicode.toString());
    }
}
