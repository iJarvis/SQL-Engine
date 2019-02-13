package dubstep.utils;

public class queryTimer {

    private Long totalTime;
    private Long start;
    private Boolean running;

    public void start() {

        assert running = false : "Start already running queryTimer";
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
