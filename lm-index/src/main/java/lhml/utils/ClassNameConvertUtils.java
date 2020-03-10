package lhml.utils;

/**
 * @FileName: ClassNameConvertUtils
 * @author: bli
 * @date: 2020年02月20日 16:48
 * @description:
 */
public class ClassNameConvertUtils {
    /**
     * 将类名首字母小写
     * @param str
     * @return
     */
    public static String lowerFirstChar(String str){
        char [] chars = str.toCharArray();
        chars[0] += 32;
        return String.valueOf(chars);
    }
}
