import system.Process;

public class Main {
    public static void main(String[] args) {

        Process t1 = new Process(5004, "teo", 1);
        Process t2 = new Process(5005, "teo", 2);
        Process t3 = new Process(5006, "teo", 3);

        t1.start();
        t2.start();
        t3.start();
    }
}
