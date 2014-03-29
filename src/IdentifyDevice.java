import java.util.*;

public class IdentifyDevice {
	private static List<String> mobile_words=Arrays.asList("mobile","iphone","iOS","ipad","android");
	public static String DeviceClassify(String source)
	{		
		for(String word:mobile_words)
		{
			if(source.contains(word))
			{
				return "mobile";
			}
		}
		return "pc";
	}
}
