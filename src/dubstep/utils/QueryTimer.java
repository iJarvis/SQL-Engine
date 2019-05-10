package dubstep.utils;

public class QueryTimer {

    private Long totalTime = 0L;
    private Long start;
    private Boolean running;

    public void start() {

        assert running = false : "Start already running QueryTimer";
        start = System.currentTimeMillis();
        running = true;

    }

    public void stop() {
        assert running == true : "Timer stop without start";
        totalTime += System.currentTimeMillis() - start;
        running = false;
    }

    public void reset() {
        running = false;
        totalTime = 0L;
    }

    public Long getTotalTime() {
        return totalTime;
    }

}
