"use client";
import { useRef, type ComponentProps } from "react";
// Touch has no hover, so holding a timeline bar opens its card until the next touch.
export function LongPressCards(props: ComponentProps<"div">) {
  const press = useRef({ timer: 0, touch: false, opened: false });
  const cancel = () => clearTimeout(press.current.timer);
  return (
    <div
      {...props}
      onPointerDown={(e) => {
        cancel();
        const card = (e.target as Element).closest(".timeline-bar")?.nextElementSibling;
        const touch = e.pointerType === "touch" && card instanceof HTMLElement;
        press.current = { timer: 0, touch, opened: false };
        if (touch)
          press.current.timer = window.setTimeout(() => {
            press.current.opened = true;
            card.showPopover();
            // A manual popover: "auto" would light-dismiss on this same press's pointerup.
            document.addEventListener("pointerdown", () => card.hidePopover(), { capture: true, once: true });
          }, 500);
      }}
      onPointerUp={cancel}
      onPointerCancel={cancel}
      // Android's link menu and a click after the press would cover the card or leave the page.
      onContextMenu={(e) => press.current.touch && e.preventDefault()}
      onClickCapture={(e) => press.current.opened && e.preventDefault()}
    />
  );
}
