import { useEffect, useRef } from 'react';

export interface FlyingMessage {
  id: string;
  fromX: number;
  fromY: number;
  toX: number;
  toY: number;
  color: string;
  duration: number;
  type: 'produce' | 'consume' | 'commit';
  label?: string;
  badgeText?: string;
  startTime: number;
}

interface AnimationLayerProps {
  animations: FlyingMessage[];
  onAnimationEnd: (id: string) => void;
}

export function AnimationLayer({ animations, onAnimationEnd }: AnimationLayerProps) {
  return (
    <div className="animation-overlay">
      {animations.map((anim) => (
        <AnimatedDot key={anim.id} anim={anim} onEnd={() => onAnimationEnd(anim.id)} />
      ))}
    </div>
  );
}

function AnimatedDot({ anim, onEnd }: { anim: FlyingMessage; onEnd: () => void }) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    // Initial position & scale
    el.style.left = `${anim.fromX}px`;
    el.style.top = `${anim.fromY}px`;
    el.style.opacity = '1';
    el.style.transform = 'translate(-50%, -50%) scale(1.3)';

    // Force reflow
    void el.getBoundingClientRect();

    const fadeDuration = Math.round(anim.duration * 0.3);
    const fadeDelay = Math.round(anim.duration * 0.7);

    requestAnimationFrame(() => {
      el.style.transition = `left ${anim.duration}ms cubic-bezier(0.25, 1, 0.5, 1), top ${anim.duration}ms cubic-bezier(0.25, 1, 0.5, 1), transform ${anim.duration}ms ease, opacity ${fadeDuration}ms ease ${fadeDelay}ms`;
      el.style.left = `${anim.toX}px`;
      el.style.top = `${anim.toY}px`;
      el.style.opacity = '0.2';
      el.style.transform = 'translate(-50%, -50%) scale(0.7)';
    });

    const timer = setTimeout(() => {
      onEnd();
    }, anim.duration + 60);

    return () => clearTimeout(timer);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const dotClass = `flying-dot flying-dot--${anim.type}`;

  return (
    <div ref={ref} className={dotClass} style={{ backgroundColor: anim.color }}>
      {anim.badgeText && <span className="flying-dot-badge">{anim.badgeText}</span>}
      {anim.label && <span className="flying-dot-label">{anim.label}</span>}
    </div>
  );
}
