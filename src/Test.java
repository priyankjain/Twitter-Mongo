import java.util.regex.Pattern;


public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//if(.matches("/satya/"))
		String str="RT @StarPlus: Cick on the VOTE FOR CHANGE button and you can pledge your support right here! #WasteIsWealth #SatyamevJayate https://t.co/nS…";
		String strptrn="satya";
		//if(Pattern.compile(strptrn, Pattern.CASE_INSENSITIVE + Pattern.LITERAL).matcher(str).find())
			if(str.toLowerCase().contains(strptrn.toLowerCase()))
			System.out.print("Match");
		else System.out.print("No Match");
	}

}
